//
// Created by liyu on 12/23/23.
//

#include "vector/TimestampColumnVector.h"
#include <iostream>
#include <sstream>
#include <ctime>

TimestampColumnVector::TimestampColumnVector(int precision, bool encoding): ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    TimestampColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, encoding);
}

TimestampColumnVector::TimestampColumnVector(uint64_t len, int precision, bool encoding): ColumnVector(len, encoding) {
    this->precision = precision;
    if(encoding) {
        posix_memalign(reinterpret_cast<void **>(&this->times), 64,
                       len * sizeof(long));
    } else {
        this->times = nullptr;
    }
}


void TimestampColumnVector::close() {
    if(!closed) {
        ColumnVector::close();
        if(encoding && this->times != nullptr) {
            free(this->times);
        }
        this->times = nullptr;
    }
}

void TimestampColumnVector::print(int rowCount) {
    throw InvalidArgumentException("not support print longcolumnvector.");
//    for(int i = 0; i < rowCount; i++) {
//        std::cout<<longVector[i]<<std::endl;
//		std::cout<<intVector[i]<<std::endl;
//    }
}

TimestampColumnVector::~TimestampColumnVector() {
    if(!closed) {
        TimestampColumnVector::close();
    }
}

void * TimestampColumnVector::current() {
    if(this->times == nullptr) {
        return nullptr;
    } else {
        return this->times + readIndex;
    }
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void TimestampColumnVector::set(int elementNum, long ts) {
    if(elementNum >= writeIndex) {
        writeIndex = elementNum + 1;
    }
    times[elementNum] = ts;
    // TODO: isNull
    isNull[elementNum] = false;
}



void TimestampColumnVector::add(std::string &value){
    std::istringstream ss(value);
    char t;
    long ts;
    struct tm time = {0};
    int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0, millis = 0, micros = 0;

    // check format
    if (!(ss >> year >> t >> month >> t >> day >> t >> hour >> t >> minute >> t >> second >> t >> millis >> micros) || t != ':' || ss.fail()) {
        throw InvalidArgumentException("Invalid timestamp format!");
    }

    time.tm_year = year - 1900;
    time.tm_mon = month - 1;
    time.tm_mday = day;
    time.tm_hour = hour;
    time.tm_min = minute;
    time.tm_sec = second;
    time.tm_isdst = -1; 


    // trans to timestamp
    ts = mktime(&time);

    if (ts == -1) {
        throw InvalidArgumentException("Error converting to timestamp!");
    }

    // unix timestamp
    long unixTs = ts * 1000'000 + millis * 1000 + micros;

    add(unixTs);
}

void TimestampColumnVector::add(int64_t value){
    if(writeIndex >= length){
        ensureSize(writeIndex * 2, true);
    }
    int index = writeIndex;
    set(index, value);
}

// void TimestampColumnVector::add(int value){
//     if(writeIndex >= length){
//         ensureSize(writeIndex * 2, true);
//     }
//     int index = writeIndex;
//     times[index] = value;
//     isNull[index] = false;
// }

void TimestampColumnVector::ensureSize(uint64_t size, bool preserveData){
	ColumnVector::ensureSize(size, preserveData);
	if(length < size){
		long *oldVector = times;
		posix_memalign(reinterpret_cast<void **>(&times), 32,
                           size * sizeof(int64_t));
		if(preserveData){
			std::copy(oldVector, oldVector + length, times);
		}
		delete[] oldVector;
		memoryUsage += (long) sizeof(long) * (size - length);
		resize(size);
	}
}