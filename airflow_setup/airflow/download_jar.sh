#!/bin/bash
LIB_DIR="/opt/airflow/lib"
CONF_FILE="/opt/airflow/jars.conf"

mkdir -p "$LIB_DIR"

if [ ! -f "$CONF_FILE" ]; then
    echo "Configuration file not found at $CONF_FILE"
    exit 1
fi

echo "Reading JAR configuration..."

sed -i 's/\r$//' "$CONF_FILE"

while IFS="|" read -r FILENAME URL || [ -n "$FILENAME" ]; do
    [[ "$FILENAME" =~ ^#.*$ ]] && continue
    [[ -z "$FILENAME" ]] && continue

    FILENAME=$(echo "$FILENAME" | tr -d '\r' | xargs)
    URL=$(echo "$URL" | tr -d '\r' | xargs)

    if [ -f "$LIB_DIR/$FILENAME" ]; then
        echo "$FILENAME already exists, skipping."
    else
        echo "Downloading $FILENAME from $URL..."
        curl -sfL "$URL" -o "$LIB_DIR/$FILENAME"
        if [ $? -eq 0 ]; then
            echo "Successfully downloaded $FILENAME"
        else
            echo "Failed to download $FILENAME. Check the URL!"
        fi
    fi
done < "$CONF_FILE"