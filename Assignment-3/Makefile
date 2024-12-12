# Variables
# MIN_CHUNK = 1024
# AVG_CHUNK = 2048
# MAX_CHUNK = 4096
# FILE_TO_UPLOAD = input.txt
# FILE_TO_DOWNLOAD = output.txt
# LOCAL_FILE_NAME = local_output.txt
# FILE_TO_DELETE = delete.txt

# Java files
JAVA_FILES = MyDedup.java Chunking.java Chunk.java Container.java FingerprintIndex.java Indexing.java WrapToContainer.java RFP.java Anchoring.java

# Targets
upload:
    javac $(JAVA_FILES)
    java MyDedup upload $(MIN_CHUNK) $(AVG_CHUNK) $(MAX_CHUNK) $(FILE_TO_UPLOAD)

download:
    javac $(JAVA_FILES)
    java MyDedup download $(FILE_TO_DOWNLOAD) $(LOCAL_FILE_NAME)

delete:
    javac $(JAVA_FILES)
    java MyDedup delete $(FILE_TO_DELETE)

clean:
    rm -f *.class