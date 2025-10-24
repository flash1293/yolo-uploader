#!/bin/bash
#
# This script reads the file path from its arguments.
#
# Arg 1: The Elasticsearch cluster URL (e.g., http://elastic:changeme@localhost:9200)
# Arg 2+: The file path(s) or glob pattern (e.g., 'logs.txt' or 'logs/*.log')
#

# --- Colors for output ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# --- Greeting ---
echo -e "${PURPLE}🚀 YOLO Log Uploader${NC}"
echo -e "${CYAN}=================================${NC}"

# --- 1. Get Arguments ---
CLUSTER_URL="$1"
shift  # Remove first argument
# All remaining arguments are file patterns/paths
FILE_ARGS=("$@")

# --- 2. Validation ---
if [ -z "$CLUSTER_URL" ]; then
  echo -e "${RED}❌ Error: Cluster URL (Arg 1) is required.${NC}" >&2
  exit 1
fi
if [ ${#FILE_ARGS[@]} -eq 0 ]; then
  echo -e "${RED}❌ Error: File path(s) (Arg 2+) required.${NC}" >&2
  exit 1
fi

# --- 3. Show what we're doing ---
echo -e "${BLUE}📡 Target:${NC} ${CLUSTER_URL}/logs"
echo -e "${YELLOW}📁 Files to upload:${NC}"

# Check if files exist and show them with line counts
files_found=false
total_lines=0

# Process all file arguments
expanded_files=""
for file in "${FILE_ARGS[@]}"; do
  if [ -f "$file" ]; then
    line_count=$(wc -l < "$file")
    echo -e "  ${GREEN}✓${NC} $file (${CYAN}${line_count}${NC} lines)"
    files_found=true
    total_lines=$((total_lines + line_count))
    expanded_files="${expanded_files}${file}"$'\n'
  fi
done

if [ "$files_found" = false ]; then
  echo -e "${RED}❌ No valid files found${NC}"
  exit 1
fi

echo -e "${YELLOW}📊 Total lines to upload: ${CYAN}${total_lines}${NC}"

# Configure batch size (lines per batch)
BATCH_SIZE=1000
if [ $total_lines -gt $BATCH_SIZE ]; then
  estimated_batches=$(( (total_lines + BATCH_SIZE - 1) / BATCH_SIZE ))
  echo -e "${BLUE}📦 Batching enabled: ${CYAN}${estimated_batches}${NC} batches of ${CYAN}${BATCH_SIZE}${NC} lines each"
fi

echo -e "${CYAN}---------------------------------${NC}"
echo -e "${YELLOW}⏳ Uploading logs...${NC}"

# --- Show preview of the request format ---
echo -e "${BLUE}📋 Request Preview (first 3 log lines):${NC}"
echo -e "${CYAN}---${NC}"

echo -e "${PURPLE}POST /logs/_bulk${NC}"

preview_count=0
echo "$expanded_files" | head -3 | xargs cat | head -3 | \
awk '{
  # Escape backslashes FIRST - use 4 backslashes to get 2 in JSON, which represents 1 literal
  gsub(/\\/, "\\\\\\\\");
  # Then escape quotes
  gsub(/"/, "\\\"");
  # Handle control characters - use octal codes and double backslash in replacement
  gsub(/\015/, "\\\\r");  # \015 is octal for carriage return (CR)
  gsub(/\012/, "\\\\n");  # \012 is octal for newline (LF)
  gsub(/\011/, "\\\\t");  # \011 is octal for tab
  gsub(/\014/, "\\\\f");  # \014 is octal for form feed
  gsub(/\010/, "\\\\b");  # \010 is octal for backspace
  printf "{\"create\":{}}\n{\"message\":\"%s\"}\n", $0
}' | while IFS= read -r line; do
  echo "$line"
done

echo -e "${CYAN}---${NC}"
echo -e "${BLUE}💡 Each log line becomes 2 JSON lines: a 'create' action + the message${NC}"
echo -e "${BLUE}🌐 Sending to: ${TARGET_URL}${NC}"
echo -e "${CYAN}---------------------------------${NC}"

# --- 4. Build full URL with fixed index ---
CLUSTER_URL_WITH_INDEX="${CLUSTER_URL}/logs"

# --- 5. Build curl options ---
TARGET_URL="${CLUSTER_URL_WITH_INDEX}/_bulk?filter_path=took,errors,items.*.error"

curl_opts=(
  -s
  -XPOST
  "$TARGET_URL"
  -H "Content-Type: application/x-ndjson"
  --data-binary @-
)

# Note: No API key support since we're using basic auth in URL

# --- 6. Run the full pipeline ---
# This command 'cats' the file(s) from Arg 2,
# pipes them to your awk command,
# and pipes the result to curl in batches.
#
# 'eval' is used so that glob patterns like 'logs/*.log' are expanded.

batch_count=0
total_processed=0
overall_success=true

# Function to process a batch
process_batch() {
  local batch_data="$1"
  local batch_num="$2"
  
  if [ -n "$batch_data" ]; then
    echo -e "${BLUE}📤 Processing batch ${batch_num}...${NC}"
    
    response=$(echo "$batch_data" | curl "${curl_opts[@]}")
    local curl_exit_code=$?
    
    if [ $curl_exit_code -eq 0 ]; then
      if echo "$response" | grep -q 'root_cause\|"errors":true'; then
        echo -e "${YELLOW}⚠️  Batch ${batch_num} had some errors${NC}"
        echo "$response"
        return 1  # Return error status instead of setting variable
      else
        echo -e "${GREEN}✅ Batch ${batch_num} completed successfully${NC}"
        return 0
      fi
    else
      echo -e "${RED}❌ Batch ${batch_num} failed!${NC}"
      echo "$response"
      return 1
    fi
  fi
  return 0
}

# Process files with batching
current_batch=""
lines_in_batch=0
temp_file=$(mktemp)

echo "$expanded_files" | xargs cat | \
awk '{
  # Escape backslashes FIRST - use 4 backslashes to get 2 in JSON, which represents 1 literal
  gsub(/\\/, "\\\\\\\\");
  # Then escape quotes
  gsub(/"/, "\\\"");
  # Handle control characters - use octal codes and double backslash in replacement
  gsub(/\015/, "\\\\r");  # \015 is octal for carriage return (CR)
  gsub(/\012/, "\\\\n");  # \012 is octal for newline (LF)
  gsub(/\011/, "\\\\t");  # \011 is octal for tab
  gsub(/\014/, "\\\\f");  # \014 is octal for form feed
  gsub(/\010/, "\\\\b");  # \010 is octal for backspace
  printf "{\"create\":{}}\n{\"message\":\"%s\"}\n", $0
}' | \
while IFS= read -r line; do
  current_batch="${current_batch}${line}\n"
  lines_in_batch=$((lines_in_batch + 1))
  total_processed=$((total_processed + 1))
  
  # Check if we've reached batch size (every 2 lines = 1 document)
  if [ $((lines_in_batch / 2)) -ge $BATCH_SIZE ]; then
    batch_count=$((batch_count + 1))
    printf "%b" "$current_batch" | process_batch "$(cat)" $batch_count
    if [ $? -ne 0 ]; then
      echo "error" >> "$temp_file"
    fi
    current_batch=""
    lines_in_batch=0
  fi
done

# Process remaining lines in final batch
if [ -n "$current_batch" ] && [ $lines_in_batch -gt 0 ]; then
  batch_count=$((batch_count + 1))
  printf "%b" "$current_batch" | process_batch "$(cat)" $batch_count
  if [ $? -ne 0 ]; then
    echo "error" >> "$temp_file"
  fi
fi

# Check if there were any errors
if [ -s "$temp_file" ]; then
  overall_success=false
fi
rm -f "$temp_file"

# Summary of batching
if [ $batch_count -gt 1 ]; then
  echo -e "${CYAN}📊 Processed ${batch_count} batches total${NC}"
fi

# --- 7. Check the result ---
if [ "$overall_success" = true ]; then
  echo -e "${GREEN}✅ All uploads completed successfully!${NC}"
  echo -e "${GREEN}🎉 All documents uploaded without errors!${NC}"
else
  echo -e "${YELLOW}⚠️  Upload completed with some warnings/errors${NC}"
  echo -e "${YELLOW}Check the batch messages above for details${NC}"
fi

echo -e "${CYAN}=================================${NC}"
echo -e "${PURPLE}✨ YOLO Upload Complete! ✨${NC}"
