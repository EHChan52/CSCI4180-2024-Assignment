# Define the Java compiler and flags
JAVAC = javac
JAVA = java
JFLAGS = 

# Define the source files and the compiled class files
SRC = MyDedup.java
CLASS_FILES = $(SRC:.java=.class)

# Default target: compile all classes
all: $(CLASS_FILES)

%.class: %.java
	$(JAVAC) $(JFLAGS) $<

# Upload target
upload: $(CLASS_FILES)
	$(JAVA) MyDedup upload <min_chunk> <avg_chunk> <max_chunk> <file_to_upload>

# Download target
download: $(CLASS_FILES)
	$(JAVA) MyDedup download <file_to_download> <local_file_name>

# Delete target
delete: $(CLASS_FILES)
	$(JAVA) MyDedup delete <file_to_delete>

# Clean target: remove compiled class files
clean:
	rm -f *.class

# Clean all target: remove compiled class files and delete data folder
clean_all:
	rm -f *.class
	rm -rf data

# Phony targets
.PHONY: all upload download delete clean clean_all