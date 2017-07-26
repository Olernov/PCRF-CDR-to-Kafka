#pragma once
#include <string>
#include <set>
#include <thread>
#include <boost/filesystem.hpp>
#include "pcrf-cdr-avro.hh"
#include "rdkafkacpp.h"

using namespace boost;


class parse_error : public std::logic_error {};


class KafkaEventCallback : public RdKafka::EventCb
{
public:
    KafkaEventCallback();
    void event_cb (RdKafka::Event &event);
private:
    bool allBrokersDown;
};


class AvroCdrCompare
{
public:
    bool operator() (const PCRF_CDR& lhs, const PCRF_CDR& rhs) const {
        if (!lhs.isSessionStart && rhs.isSessionStart) return true;
        else if (lhs.isSessionStart && !rhs.isSessionStart) return false;

        if (lhs.sessionID < rhs.sessionID) return true;
        else if (lhs.sessionID > rhs.sessionID) return false;

        if (lhs.eventTime < rhs.eventTime) return true;
        else if (lhs.eventTime > rhs.eventTime) return false;

        if ((lhs.iMSI.is_null() ? 0LL : lhs.iMSI.get_long()) <
            (rhs.iMSI.is_null() ? 0LL : rhs.iMSI.get_long())) return true;
        else if ((lhs.iMSI.is_null() ? 0LL : lhs.iMSI.get_long()) >
            (rhs.iMSI.is_null() ? 0LL : rhs.iMSI.get_long())) return false;

        if ((lhs.iPAddress.is_null() ? 0LL : lhs.iPAddress.get_long()) <
            (rhs.iPAddress.is_null() ? 0LL : rhs.iPAddress.get_long())) return true;
        else if ((lhs.iPAddress.is_null() ? 0LL : lhs.iPAddress.get_long()) >
            (rhs.iPAddress.is_null() ? 0LL : rhs.iPAddress.get_long())) return false;

        if ((lhs.iMEI.is_null() ? std::string("<null>") : lhs.iMEI.get_string()) <
            (rhs.iMEI.is_null() ? std::string("<null>") : rhs.iMEI.get_string())) return true;
        else if ((lhs.iMEI.is_null() ? std::string("<null>") : lhs.iMEI.get_string()) >
                 (rhs.iMEI.is_null() ? std::string("<null>") : rhs.iMEI.get_string())) return false;

        if ((lhs.aPN.is_null() ? std::string("<null>") : lhs.aPN.get_string()) <
            (rhs.aPN.is_null() ? std::string("<null>") : rhs.aPN.get_string())) return true;
        else if ((lhs.aPN.is_null() ? std::string("<null>") : lhs.aPN.get_string()) >
                 (rhs.aPN.is_null() ? std::string("<null>") : rhs.aPN.get_string())) return false;

        return false;
    }
};


class Parser
{
public:
    Parser(const std::string &kafkaBroker, const std::string &kafkaTopic, uint32_t partition,
           const std::string &cdrFilesDirectory, const std::string &cdrExtension,
           const std::string &archiveDirectory, const std::string &cdrBadDirectory,
           bool runtest);
    ~Parser();
    void ProcessFile(const filesystem::path& file);
    void SetStopFlag() { stopFlag = true; }
    bool IsReady();
    const std::string& GetPostponeReason() const { return postponeReason; }

private:
    std::string kafkaBroker;
    std::string kafkaTopic;
    uint32_t kafkaPartition;
    std::string cdrArchiveDirectory;
    std::string cdrBadDirectory;
    std::string postponeReason;
    const int producerPollTimeoutMs = 3000;
    const int queueSizeThreshold = 20;
    std::string shutdownFilePath;
    std::string lastErrorMessage;

    bool printFileContents;
    bool runTest;
    bool stopFlag;
    std::unique_ptr<RdKafka::Conf> kafkaGlobalConf;
    std::unique_ptr<RdKafka::Conf> kafkaTopicConf;
    std::unique_ptr<RdKafka::Producer> kafkaProducer;
    KafkaEventCallback eventCb;
    std::multiset<PCRF_CDR, AvroCdrCompare> sentAvroCdrs;

    uint32_t ParseFile(std::ifstream &cdrStream, const std::string& filename);
    void WaitForKafkaQueue();
    void SendRecordToKafka(const PCRF_CDR &pcrfCdr);
    bool CompareSentAndConsumedRecords(int64_t startOffset);
    void PrintAvroCdrContents(const PCRF_CDR& cdr) const;
    void RunTests();
    int EncodeAvro(const PCRF_CDR& avroCdr, avro::OutputStream* out);
    void ReadEncodedAvroCdr(avro::OutputStream* out, size_t byteCount, std::vector<uint8_t> &rawData);
    std::vector<uint8_t> EncodeCdr(const PCRF_CDR& avroCdr);
    int64_t TextualTimeToUnixTime(const std::string& textualEventTime);
    int64_t TextualIPAddrToInt(const std::string& textualIpAddr);
    void LogParseError(const std::string& line, const std::string& errDescr);
};


