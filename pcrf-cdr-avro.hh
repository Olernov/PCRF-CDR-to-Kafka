/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#ifndef PCRF_CDR_AVRO_2960801664__H_
#define PCRF_CDR_AVRO_2960801664__H_


#include <sstream>
#include "boost/any.hpp"
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"

struct pcrf_cdr_avro_schema_avsc_Union__0__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    bool is_null() const {
        return (idx_ == 0);
    }
    void set_null() {
        idx_ = 0;
        value_ = boost::any();
    }
    int64_t get_long() const;
    void set_long(const int64_t& v);
    pcrf_cdr_avro_schema_avsc_Union__0__();
};

struct pcrf_cdr_avro_schema_avsc_Union__1__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    bool is_null() const {
        return (idx_ == 0);
    }
    void set_null() {
        idx_ = 0;
        value_ = boost::any();
    }
    int64_t get_long() const;
    void set_long(const int64_t& v);
    pcrf_cdr_avro_schema_avsc_Union__1__();
};

struct pcrf_cdr_avro_schema_avsc_Union__2__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    bool is_null() const {
        return (idx_ == 0);
    }
    void set_null() {
        idx_ = 0;
        value_ = boost::any();
    }
    std::string get_string() const;
    void set_string(const std::string& v);
    pcrf_cdr_avro_schema_avsc_Union__2__();
};

struct pcrf_cdr_avro_schema_avsc_Union__3__ {
private:
    size_t idx_;
    boost::any value_;
public:
    size_t idx() const { return idx_; }
    bool is_null() const {
        return (idx_ == 0);
    }
    void set_null() {
        idx_ = 0;
        value_ = boost::any();
    }
    std::string get_string() const;
    void set_string(const std::string& v);
    pcrf_cdr_avro_schema_avsc_Union__3__();
};

struct PCRF_CDR {
    typedef pcrf_cdr_avro_schema_avsc_Union__0__ iMSI_t;
    typedef pcrf_cdr_avro_schema_avsc_Union__1__ iPAddress_t;
    typedef pcrf_cdr_avro_schema_avsc_Union__2__ iMEI_t;
    typedef pcrf_cdr_avro_schema_avsc_Union__3__ aPN_t;
    bool isSessionStart;
    std::string sessionID;
    int64_t eventTime;
    iMSI_t iMSI;
    iPAddress_t iPAddress;
    iMEI_t iMEI;
    aPN_t aPN;
    PCRF_CDR() :
        isSessionStart(bool()),
        sessionID(std::string()),
        eventTime(int64_t()),
        iMSI(iMSI_t()),
        iPAddress(iPAddress_t()),
        iMEI(iMEI_t()),
        aPN(aPN_t())
        { }
};

inline
int64_t pcrf_cdr_avro_schema_avsc_Union__0__::get_long() const {
    if (idx_ != 1) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<int64_t >(value_);
}

inline
void pcrf_cdr_avro_schema_avsc_Union__0__::set_long(const int64_t& v) {
    idx_ = 1;
    value_ = v;
}

inline
int64_t pcrf_cdr_avro_schema_avsc_Union__1__::get_long() const {
    if (idx_ != 1) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<int64_t >(value_);
}

inline
void pcrf_cdr_avro_schema_avsc_Union__1__::set_long(const int64_t& v) {
    idx_ = 1;
    value_ = v;
}

inline
std::string pcrf_cdr_avro_schema_avsc_Union__2__::get_string() const {
    if (idx_ != 1) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::string >(value_);
}

inline
void pcrf_cdr_avro_schema_avsc_Union__2__::set_string(const std::string& v) {
    idx_ = 1;
    value_ = v;
}

inline
std::string pcrf_cdr_avro_schema_avsc_Union__3__::get_string() const {
    if (idx_ != 1) {
        throw avro::Exception("Invalid type for union");
    }
    return boost::any_cast<std::string >(value_);
}

inline
void pcrf_cdr_avro_schema_avsc_Union__3__::set_string(const std::string& v) {
    idx_ = 1;
    value_ = v;
}

inline pcrf_cdr_avro_schema_avsc_Union__0__::pcrf_cdr_avro_schema_avsc_Union__0__() : idx_(0) { }
inline pcrf_cdr_avro_schema_avsc_Union__1__::pcrf_cdr_avro_schema_avsc_Union__1__() : idx_(0) { }
inline pcrf_cdr_avro_schema_avsc_Union__2__::pcrf_cdr_avro_schema_avsc_Union__2__() : idx_(0) { }
inline pcrf_cdr_avro_schema_avsc_Union__3__::pcrf_cdr_avro_schema_avsc_Union__3__() : idx_(0) { }
namespace avro {
template<> struct codec_traits<pcrf_cdr_avro_schema_avsc_Union__0__> {
    static void encode(Encoder& e, pcrf_cdr_avro_schema_avsc_Union__0__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            e.encodeNull();
            break;
        case 1:
            avro::encode(e, v.get_long());
            break;
        }
    }
    static void decode(Decoder& d, pcrf_cdr_avro_schema_avsc_Union__0__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            d.decodeNull();
            v.set_null();
            break;
        case 1:
            {
                int64_t vv;
                avro::decode(d, vv);
                v.set_long(vv);
            }
            break;
        }
    }
};

template<> struct codec_traits<pcrf_cdr_avro_schema_avsc_Union__1__> {
    static void encode(Encoder& e, pcrf_cdr_avro_schema_avsc_Union__1__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            e.encodeNull();
            break;
        case 1:
            avro::encode(e, v.get_long());
            break;
        }
    }
    static void decode(Decoder& d, pcrf_cdr_avro_schema_avsc_Union__1__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            d.decodeNull();
            v.set_null();
            break;
        case 1:
            {
                int64_t vv;
                avro::decode(d, vv);
                v.set_long(vv);
            }
            break;
        }
    }
};

template<> struct codec_traits<pcrf_cdr_avro_schema_avsc_Union__2__> {
    static void encode(Encoder& e, pcrf_cdr_avro_schema_avsc_Union__2__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            e.encodeNull();
            break;
        case 1:
            avro::encode(e, v.get_string());
            break;
        }
    }
    static void decode(Decoder& d, pcrf_cdr_avro_schema_avsc_Union__2__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            d.decodeNull();
            v.set_null();
            break;
        case 1:
            {
                std::string vv;
                avro::decode(d, vv);
                v.set_string(vv);
            }
            break;
        }
    }
};

template<> struct codec_traits<pcrf_cdr_avro_schema_avsc_Union__3__> {
    static void encode(Encoder& e, pcrf_cdr_avro_schema_avsc_Union__3__ v) {
        e.encodeUnionIndex(v.idx());
        switch (v.idx()) {
        case 0:
            e.encodeNull();
            break;
        case 1:
            avro::encode(e, v.get_string());
            break;
        }
    }
    static void decode(Decoder& d, pcrf_cdr_avro_schema_avsc_Union__3__& v) {
        size_t n = d.decodeUnionIndex();
        if (n >= 2) { throw avro::Exception("Union index too big"); }
        switch (n) {
        case 0:
            d.decodeNull();
            v.set_null();
            break;
        case 1:
            {
                std::string vv;
                avro::decode(d, vv);
                v.set_string(vv);
            }
            break;
        }
    }
};

template<> struct codec_traits<PCRF_CDR> {
    static void encode(Encoder& e, const PCRF_CDR& v) {
        avro::encode(e, v.isSessionStart);
        avro::encode(e, v.sessionID);
        avro::encode(e, v.eventTime);
        avro::encode(e, v.iMSI);
        avro::encode(e, v.iPAddress);
        avro::encode(e, v.iMEI);
        avro::encode(e, v.aPN);
    }
    static void decode(Decoder& d, PCRF_CDR& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.isSessionStart);
                    break;
                case 1:
                    avro::decode(d, v.sessionID);
                    break;
                case 2:
                    avro::decode(d, v.eventTime);
                    break;
                case 3:
                    avro::decode(d, v.iMSI);
                    break;
                case 4:
                    avro::decode(d, v.iPAddress);
                    break;
                case 5:
                    avro::decode(d, v.iMEI);
                    break;
                case 6:
                    avro::decode(d, v.aPN);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.isSessionStart);
            avro::decode(d, v.sessionID);
            avro::decode(d, v.eventTime);
            avro::decode(d, v.iMSI);
            avro::decode(d, v.iPAddress);
            avro::decode(d, v.iMEI);
            avro::decode(d, v.aPN);
        }
    }
};

}
#endif
