#include <memory>
#include "Parser.h"
#include "LogWriter.h"
#include "Config.h"


extern Config config;
extern LogWriter logWriter;


KafkaEventCallback::KafkaEventCallback() :
    allBrokersDown(false)
{}

void KafkaEventCallback::event_cb (RdKafka::Event &event)
{
    switch (event.type())
    {
      case RdKafka::Event::EVENT_ERROR:
        logWriter << "Kafka ERROR (" + RdKafka::err2str(event.err()) + "): " + event.str();
        if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN)
            allBrokersDown = true;
        break;
      case RdKafka::Event::EVENT_STATS:
        logWriter << "Kafka STATS: " + event.str();
        break;
      case RdKafka::Event::EVENT_LOG:
        logWriter << "Kafka LOG-" + std::to_string(event.severity()) + "-" + event.fac() + ":" + event.str();
        break;
      default:
        logWriter << "Kafka EVENT " + std::to_string(event.type()) + " (" +
                     RdKafka::err2str(event.err()) + "): " + event.str();
        break;
    }
}


Parser::Parser(const std::string &kafkaBroker, const std::string &kafkaTopic, uint32_t partition,
               const std::string &filesDirectory, const std::string &extension,
               const std::string &archDirectory, const std::string &badDirectory, bool runtest) :
    kafkaTopic(kafkaTopic),
    kafkaPartition(partition),
    cdrArchiveDirectory(archDirectory),
    cdrBadDirectory(badDirectory),
    printFileContents(false),
    runTest(runtest),
    stopFlag(false),
    kafkaGlobalConf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL)),
    kafkaTopicConf(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC))
{

    std::string errstr;
    if (kafkaGlobalConf->set("bootstrap.servers", kafkaBroker, errstr) != RdKafka::Conf::CONF_OK
            || kafkaGlobalConf->set("group.id", "pgw-emitter", errstr) != RdKafka::Conf::CONF_OK
            || kafkaGlobalConf->set("api.version.request", "true", errstr) != RdKafka::Conf::CONF_OK) {
        throw std::runtime_error("Failed to set kafka global conf: " + errstr);
    }
    kafkaGlobalConf->set("event_cb", &eventCb, errstr);

    kafkaProducer = std::unique_ptr<RdKafka::Producer>(RdKafka::Producer::create(kafkaGlobalConf.get(), errstr));
    if (!kafkaProducer) {
        throw std::runtime_error("Failed to create kafka producer: " + errstr);
    }
    if (runTest) {
        RunTests();
    }
}


bool Parser::IsReady()
{
    postponeReason.clear();
    kafkaProducer->poll(0);
    if (kafkaProducer->outq_len() > 0) {
        postponeReason = std::to_string(kafkaProducer->outq_len()) + " unsent messages in Kafka queue";
    }

    return postponeReason.empty();
}

void Parser::ProcessFile(const filesystem::path& file)
{
    std::ifstream cdrStream(file.string(), std::ifstream::in);
    if(!cdrStream.is_open()) {
        logWriter.Write("Unable to open input file " + file.string() + ". Skipping it." , LogWriter::mainThr, error);
        return;
    }

    int64_t lowOffset = 0;
    int64_t highOffset = 0;
    if (runTest) {
        RdKafka::ErrorCode res = kafkaProducer->query_watermark_offsets (kafkaTopic, kafkaPartition, &lowOffset, &highOffset, 1000);
        if (res != RdKafka::ERR_NO_ERROR) {
            std::cout << "Kafka query_watermark_offsets failed: " + RdKafka::err2str(res) << std::endl;
            assert(false);
        }
        std::cout << "Kafka low watermark: " << lowOffset << std::endl;
        std::cout << "Kafka high watermark: " << highOffset << std::endl;
        sentAvroCdrs.clear();
    }

    logWriter << "Processing file " + file.filename().string() + "...";
    time_t processStartTime;
    time(&processStartTime);
    uint32_t recordCount = 0;
    try {
        recordCount = ParseFile(cdrStream, file.string());
        if (!cdrArchiveDirectory.empty()) {
            filesystem::path archivePath(cdrArchiveDirectory);
            filesystem::path archiveFilename = archivePath / file.filename();
            filesystem::rename(file, archiveFilename);
        }
    }
    catch(const std::exception& ex) {
        logWriter.Write("ERROR while ProcessFile:", LogWriter::mainThr, error);
        logWriter.Write(ex.what(), LogWriter::mainThr, error);
        if (!cdrBadDirectory.empty()) {
            filesystem::path badFilePath(cdrBadDirectory);
            filesystem::path badFilename = badFilePath / file.filename();
            filesystem::rename(file, badFilename);
            logWriter << "File " + file.filename().string() + " moved to bad files directory " + cdrBadDirectory;
        }
    }
    long processTimeSec = difftime(time(nullptr), processStartTime);
    logWriter << "File " + file.filename().string() + " having " +std::to_string(recordCount) +
                 " records processed in " +
                 std::to_string(processTimeSec) + " sec.";

    if (runTest) {
        std::cout << "sentAvroCdrs.size(): "  << sentAvroCdrs.size() << ", recordCount: " <<recordCount <<std::endl;
        assert(sentAvroCdrs.size() == recordCount);
        std::cout<< "Consuming sent records from Kafka ..." << std::endl;
        assert(CompareSentAndConsumedRecords(highOffset));
    }
    cdrStream.close();
}


uint32_t Parser::ParseFile(std::ifstream& cdrStream, const std::string& filename)
{
    std::string line;
    int recordCount  = 0;
    while(std::getline(cdrStream, line)) {
        recordCount++;
        std::istringstream iss(line);
        PCRF_CDR avroCdr;
        std::string recordType, textualEventTime;
        if (!(iss >> recordType >> avroCdr.sessionID >> textualEventTime)) {
            LogParseError(line, "Could not extract recordType, sessionID, textualEventTime");
            continue;
        }
        if (recordType == "start") {
            avroCdr.isSessionStart = true;
        }
        else if (recordType == "stop") {
            avroCdr.isSessionStart = false;
        }
        else {
            LogParseError(line, "Record type differs from start/stop");
            continue;
        }
        avroCdr.eventTime = TextualTimeToUnixTime(textualEventTime);
        if (avroCdr.isSessionStart) {
            int64_t imsi;
            std::string textualIpAddr, imei, apn;
            if (!(iss >> imsi >> textualIpAddr)) {
                LogParseError(line, "Could not extract imsi, textualIpAddr");
                continue;
            }
            std::string dummy;
            getline(iss,   dummy, '\t');
            getline(iss,   imei, '\t');
            getline(iss,   apn, '\t');
            if (apn.empty()) {
                LogParseError(line, "Could not extract APN");
                continue;
            }
            avroCdr.iMSI.set_long(imsi);
            avroCdr.iPAddress.set_long( TextualIPAddrToInt(textualIpAddr) );
            if (!imei.empty()) {
                avroCdr.iMEI.set_string(imei);
            }
            else {
                avroCdr.iMEI.set_null();
            }
            avroCdr.aPN.set_string(apn);
        }

        SendRecordToKafka(avroCdr);
    }
    kafkaProducer->poll(0);
    return recordCount;
}


void Parser::SendRecordToKafka(const PCRF_CDR& pcrfCdr)
{
    std::vector<uint8_t> rawData = EncodeCdr(pcrfCdr);
    RdKafka::ErrorCode resp;
    std::string errstr;
    do {
        resp = kafkaProducer->produce(kafkaTopic, RdKafka::Topic::PARTITION_UA,
                               RdKafka::Producer::RK_MSG_COPY,
                               rawData.data(), rawData.size(), nullptr, 0,
                               time(nullptr) * 1000 /*milliseconds*/, nullptr);
        if (resp == RdKafka::ERR__QUEUE_FULL) {
            kafkaProducer->poll(producerPollTimeoutMs);
        }
    }
    while (resp == RdKafka::ERR__QUEUE_FULL);

    if (resp != RdKafka::ERR_NO_ERROR) {
        throw std::runtime_error("Kafka produce failed: " + RdKafka::err2str(resp));
    }
    else {
        if (runTest) {
            auto res = sentAvroCdrs.insert(pcrfCdr);
            auto it = sentAvroCdrs.find(pcrfCdr);
            assert (it != sentAvroCdrs.end());
        }
    }
}


int Parser::EncodeAvro(const PCRF_CDR &avroCdr, avro::OutputStream* out)
{
    avro::EncoderPtr encoder(avro::binaryEncoder());
    encoder->init(*out);
    avro::encode(*encoder, avroCdr);
    encoder->flush();
    return out->byteCount();
}


void Parser::ReadEncodedAvroCdr(avro::OutputStream* out, size_t byteCount, std::vector<uint8_t>& rawData)
{
    std::unique_ptr<avro::InputStream> in = avro::memoryInputStream(*out);
    avro::StreamReader reader(*in);
    reader.readBytes(&rawData[0], byteCount);
}


std::vector<uint8_t> Parser::EncodeCdr(const PCRF_CDR &avroCdr)
{
    std::unique_ptr<avro::OutputStream> out(avro::memoryOutputStream());
    size_t byteCount = EncodeAvro(avroCdr, out.get());
    std::vector<uint8_t> rawData(byteCount);
    ReadEncodedAvroCdr(out.get(), byteCount, rawData);
    return rawData;
}


void Parser::WaitForKafkaQueue()
{
    while (kafkaProducer->outq_len() > 0)   {
        std::string message = std::to_string(kafkaProducer->outq_len()) + " message(s) are in Kafka producer queue. "
                "Waiting to be sent...";
        if (message != lastErrorMessage) {
            logWriter << message;
            lastErrorMessage = message;
        }
        kafkaProducer->poll(producerPollTimeoutMs);
    }
}


void Parser::LogParseError(const std::string& line, const std::string& errDescr)
{
    logWriter.Write("Next line has bad format:", LogWriter::mainThr, error);
    logWriter.Write(line, LogWriter::mainThr, error);
    logWriter.Write(errDescr, LogWriter::mainThr, error);
    if (runTest) {
        std::cout << "Next line has bad format:" << std::endl;
        std::cout << line  << std::endl;
        std::cout << errDescr << std::endl;
    }
}


bool Parser::CompareSentAndConsumedRecords(int64_t startOffset)
{
    std::string errstr;

    std::unique_ptr<RdKafka::Consumer> consumer(RdKafka::Consumer::create(kafkaGlobalConf.get(), errstr));
    if (!consumer) {
      std::cout << "Failed to create consumer: " << errstr << std::endl;
      return false;
    }

    std::unique_ptr<RdKafka::Topic> topic(RdKafka::Topic::create(consumer.get(), kafkaTopic,
                           kafkaTopicConf.get(), errstr));
    if (!topic) {
      std::cout << "Failed to create topic: " << errstr << std::endl;
      return false;
    }

    RdKafka::ErrorCode resp = consumer->start(topic.get(), kafkaPartition, startOffset);
    if (resp != RdKafka::ERR_NO_ERROR) {
      std::cout << "Failed to start consumer: " << RdKafka::err2str(resp) << std::endl;
      return false;
    }

    uint32_t consumed = 0;
    bool failedToFindCdr = false;
    while(true) {
        std::unique_ptr<RdKafka::Message> message(consumer->consume(topic.get(), kafkaPartition, 5000));
        RdKafka::MessageTimestamp mt = message->timestamp();
        if (message->err() == RdKafka::ERR__TIMED_OUT) {
            // consider we have read all records
            break;
        }
        if (message->err() == RdKafka::ERR_NO_ERROR) {
            consumed++;
            PCRF_CDR avroCdr;
            std::unique_ptr<avro::InputStream> in(avro::memoryInputStream(
                                         static_cast<uint8_t*>(message->payload()), message->len()));
            avro::DecoderPtr decoder(avro::binaryDecoder());
            decoder->init(*in);
            avro::decode(*decoder, avroCdr);
            auto it = sentAvroCdrs.find(avroCdr);
            if (it != sentAvroCdrs.end()) {
                sentAvroCdrs.erase(it);

            }
            else {
                std::cout << std::endl << "CDR NOT FOUND in sent records:" << std::endl;
                PrintAvroCdrContents(avroCdr);
                failedToFindCdr = true;
            }
        }
        else {
           switch (message->err()) {
              case RdKafka::ERR__PARTITION_EOF:
                break;

              case RdKafka::ERR__UNKNOWN_TOPIC:
              case RdKafka::ERR__UNKNOWN_PARTITION:
                std::cout << "Consume failed: " << message->errstr() << std::endl;
                break;

              default:
                /* Errors */
                std::cout << "Consume failed: " << message->errstr() << std::endl;
            }
        }
        consumer->poll(0);
    }

    consumer->stop(topic.get(), kafkaPartition);
    consumer->poll(1000);
    std::cout << "Consumed " << consumed << " records from Kafka." << std::endl;
    std::cout << "sentAvroCdrs left: " << sentAvroCdrs.size() << std::endl;
    std::cout << (sentAvroCdrs.size() > 0 ? "CompareSentAndConsumedRecords test FAILED" :
                                            "CompareSentAndConsumedRecords test PASSED") << std::endl;
    return !failedToFindCdr && (sentAvroCdrs.size() == 0);
}


void Parser::PrintAvroCdrContents(const PCRF_CDR& cdr) const
{
    std::cout << "**** Avro CDR contents: ****" << std::endl
              << "isSessionStart: " << (cdr.isSessionStart ? "true" : "false") << std::endl
              << "sessionID: " << cdr.sessionID << std::endl
             << "eventTime: " << cdr.eventTime << std::endl
            << "IMSI: " <<  (cdr.iMSI.is_null() ? std::string("<null>") :
                        std::to_string(cdr.iMSI.get_long())) << std::endl
           << "iPAddress: " << (cdr.iPAddress.is_null() ? std::string("<null>") :
                        std::to_string(cdr.iPAddress.get_long())) << std::endl
            << "IMEI: " << (cdr.iMEI.is_null() ? std::string("<null>") : cdr.iMEI.get_string()) << std::endl
            << "APN: " << (cdr.aPN.is_null() ? std::string("<null>") : cdr.aPN.get_string()) << std::endl;
}


int64_t Parser::TextualTimeToUnixTime(const std::string& textTime)
{
    tm result;
    const char* start = textTime.c_str();
    char* end;
    result.tm_year = strtol(start, &end, 10) - 1900; // years since 1900
    result.tm_mon = strtol(end+1, &end, 10) - 1; // months since January (0-11)
    result.tm_mday = strtol(end+1, &end, 10); // day of the month (1-31)
    result.tm_hour = strtol(end+1, &end, 10); // hours since midnight (0-23)
    result.tm_min = strtol(end+1, &end, 10); // minutes after the hour (0-59)
    result.tm_sec = strtol(end+1, nullptr, 10); // seconds after minute(0-61)
    result.tm_wday = 0; // not used
    result.tm_yday = 0; // not used
    result.tm_isdst = 0; //Daylight saving Time flag
    return mktime(&result);
}


int64_t Parser::TextualIPAddrToInt(const std::string& textualIpAddr)
{
    int64_t ip = 0;
    const char* start = textualIpAddr.c_str();
    char* end = const_cast<char*>(start - 1);
    for (int i = 0; i < 4; i++) {
        ip <<= 8;
        ip += strtol(end + 1, &end, 10);
    }
    return ip;
}


Parser::~Parser()
{
    WaitForKafkaQueue();
    RdKafka::wait_destroyed(5000);
}


void Parser::RunTests()
{
    sentAvroCdrs.clear();
    PCRF_CDR emptyCdr, cdr, cdr3;
    auto res = sentAvroCdrs.insert(emptyCdr);
    auto it = sentAvroCdrs.find(emptyCdr);
    assert(it != sentAvroCdrs.end());

    cdr.isSessionStart = true;
    cdr.sessionID = "ugw2;123;456;789789";
    cdr.eventTime = TextualTimeToUnixTime("2017.07.21_11:13:30");
    cdr.iMSI.set_long(250270100113797);
    cdr.iPAddress.set_long(TextualIPAddrToInt("10.184.205.13"));
    cdr.iMEI.set_string("333456456456456");
    cdr.aPN.set_string("letai.internet.ru");
    it = sentAvroCdrs.find(cdr);
    assert(it == sentAvroCdrs.end());

    sentAvroCdrs.insert(cdr);
    it = sentAvroCdrs.find(cdr);
    assert(it != sentAvroCdrs.end());

    PCRF_CDR cdr2;
    cdr2.isSessionStart = false;
    cdr2.sessionID = "ugw2;123;456;789789";
    cdr2.eventTime = TextualTimeToUnixTime("2017.07.21_12:54:30");

    sentAvroCdrs.insert(cdr2);
    it = sentAvroCdrs.find(cdr2);
    assert(it != sentAvroCdrs.end());
    assert(sentAvroCdrs.size() == 3);

    std::unique_ptr<avro::OutputStream> out(avro::memoryOutputStream());
    avro::EncoderPtr encoder(avro::binaryEncoder());
    encoder->init(*out);
    avro::encode(*encoder, cdr);
    encoder->flush();

    std::unique_ptr<avro::InputStream> in(avro::memoryInputStream(*out));
    avro::DecoderPtr decoder(avro::binaryDecoder());
    decoder->init(*in);
    avro::decode(*decoder, cdr3);
    it = sentAvroCdrs.find(cdr3);
    assert(it != sentAvroCdrs.end());

    sentAvroCdrs.clear();
    std::cout << "Avro encoder/decoder tests PASSED. Starting to send sample CDR set. " << std::endl;

    std::vector<uint8_t> rawData = EncodeCdr(cdr);
    std::vector<uint8_t> rawData2 = EncodeCdr(cdr2);
    RdKafka::ErrorCode resp;
    std::string errstr;
    resp = kafkaProducer->produce(kafkaTopic, RdKafka::Topic::PARTITION_UA,
                               RdKafka::Producer::RK_MSG_COPY,
                               rawData.data(), rawData.size(), nullptr, 0,
                               time(nullptr) * 1000 /*milliseconds*/, nullptr);
    assert(resp == RdKafka::ERR_NO_ERROR);

    time_t now = time(nullptr);
    std::cout << "1st session start sent at " << asctime(localtime(&now)) << std::endl;

    now = time(nullptr);
    std::cout << "Sending 1000 CDRs start/stop in 10 seconds started at " << asctime(localtime(&now)) << std::endl;
    for (int i = 0; i < 1000; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        resp = kafkaProducer->produce(kafkaTopic, RdKafka::Topic::PARTITION_UA,
                                   RdKafka::Producer::RK_MSG_COPY,
                                   rawData.data(), rawData.size(), nullptr, 0,
                                   time(nullptr) * 1000 /*milliseconds*/, nullptr);
        assert(resp == RdKafka::ERR_NO_ERROR);

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        resp = kafkaProducer->produce(kafkaTopic, RdKafka::Topic::PARTITION_UA,
                                   RdKafka::Producer::RK_MSG_COPY,
                                   rawData2.data(), rawData2.size(), nullptr, 0,
                                   time(nullptr) * 1000 /*milliseconds*/, nullptr);
        assert(resp == RdKafka::ERR_NO_ERROR);
    }

    // send unregistered IMSI
    std::cout << "Sending unregistered IMSI 250270100426286" << std::endl;
    cdr.iMSI.set_long(250270100426286);
    rawData = EncodeCdr(cdr);
    resp = kafkaProducer->produce(kafkaTopic, RdKafka::Topic::PARTITION_UA,
                               RdKafka::Producer::RK_MSG_COPY,
                               rawData.data(), rawData.size(), nullptr, 0,
                               time(nullptr) * 1000 /*milliseconds*/, nullptr);
    assert(resp == RdKafka::ERR_NO_ERROR);

    // registered IMSI again
    cdr.iMSI.set_long(250270100113797);
    rawData = EncodeCdr(cdr);
    resp = kafkaProducer->produce(kafkaTopic, RdKafka::Topic::PARTITION_UA,
                               RdKafka::Producer::RK_MSG_COPY,
                               rawData.data(), rawData.size(), nullptr, 0,
                               time(nullptr) * 1000 /*milliseconds*/, nullptr);
    assert(resp == RdKafka::ERR_NO_ERROR);

    std::cout << "Sample CDR set set is sent to topic: " << kafkaTopic << std::endl
        << "Check consumed records and field values at consuming service." << std::endl
        << "There must be no unregistered  IMSI 250270100426286" << std::endl
        << "Put some files to INPUT_DIR to perform Kafka produce/consume tests." << std::endl;
}
