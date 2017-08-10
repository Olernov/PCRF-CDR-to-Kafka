#pragma once
#include "Parser.h"

class MainLoopController
{
public:
    MainLoopController(const std::string &kafkaBroker, const std::string &kafkaTopic, uint32_t kafkaPartition,
                       const std::string &cdrFilesDirectory,
                       const std::string &cdrExtension, const std::string &archiveDirectory,
                       const std::string &cdrBadDirectory,
                       bool runTests);
    void Run();
    ~MainLoopController();
private:
    typedef std::vector<filesystem::path> fileList;
    const int secondsToSleepWhenNothingToDo = 1;
    Parser parser;
    std::string cdrFilesDirectory;
    std::string cdrExtension;

    const std::string shutdownFlagFilename = "pcrf-to-kafka.stop";
    std::string shutdownFilePath;

    bool printFileContents;
    bool stopFlag;

    void ConstructSortedFileList(const std::string& inputDir,
                                 const std::string &cdrExtension, fileList &sourceFiles);
    bool IsShutdownFlagSet();
    void Sleep();
};


