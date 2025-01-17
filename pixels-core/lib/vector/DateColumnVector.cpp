//
// Created by yuly on 06.04.23.
//

#include "vector/DateColumnVector.h"
#include <chrono>
#include <iostream>
#include <sstream>
#include <ctime>

DateColumnVector::DateColumnVector(uint64_t len, bool encoding): ColumnVector(len, encoding) {
	if(encoding) {
        posix_memalign(reinterpret_cast<void **>(&dates), 32,
                       len * sizeof(int32_t));
	} else {
		this->dates = nullptr;
	}
	memoryUsage += (long) sizeof(int) * len;
}

void DateColumnVector::close() {
	if(!closed) {
		if(encoding && dates != nullptr) {
			free(dates);
		}
		dates = nullptr;
		ColumnVector::close();
	}
}

void DateColumnVector::print(int rowCount) {
	for(int i = 0; i < rowCount; i++) {
		std::cout<<dates[i]<<std::endl;
	}
}

DateColumnVector::~DateColumnVector() {
	if(!closed) {
		DateColumnVector::close();
	}
}

/**
     * Set a row from a value, which is the days from 1970-1-1 UTC.
     * We assume the entry has already been isRepeated adjusted.
     *
     * @param elementNum
     * @param days
 */
void DateColumnVector::set(int elementNum, int days) {
	if(elementNum >= writeIndex) {
		writeIndex = elementNum + 1;
	}
	dates[elementNum] = days;
	// TODO: isNull
	isNull[elementNum] = false;
}

void * DateColumnVector::current() {
    if(dates == nullptr) {
        return nullptr;
    } else {
        return dates + readIndex;
    }
}

void DateColumnVector::print(int rowCount) {
	for(int i = 0; i < rowCount; i++) {
		std::cout<<dates[i]<<std::endl;
	}
}

//check date format
bool isValidDateFormat(const std::string &val, int &year, int &month, int &day) {
    std::istringstream ss(val);
    char dash1, dash2;
    ss >> year >> dash1 >> month >> dash2 >> day;
    return !ss.fail() && dash1 == '-' && dash2 == '-';
}

long getTime(){
	std::tm day = {0};
	day.tm_year = 70; //1970
	day.tm_mon = 0; //January
	day.tm_mday = 1; //1st day
	return mktime(&day);
}

long convertToDays(const std::string &value){
	int year, month, day;
	if(!isValidDateFormat(value, year, month, day)){
		std::cerr << "Invalid date format!" << std::endl;
        return -1;
	}
	std::tm date = {};
	date.tm_year = year - 1900;
	date.tm_mon = month - 1;
	date.tm_mday = day;
	date.tm_isdst = -1;

	time_t time = mktime(&date);
	long start = getTime();
	std::chrono::system_clock::time_point tp = std::chrono::system_clock::from_time_t(time);
    std::chrono::system_clock::time_point start_tp = std::chrono::system_clock::from_time_t(start);

	return std::chrono::duration_cast<std::chrono::hours>(tp - start_tp).count() / 24;
}

void DateColumnVector::add(std::string &value){
	long days = convertToDays(value);
	if(days != -1)
		this->add((int)days);
}

void DateColumnVector::add(bool value){
	add(value ? 1 : 0);
}

void DateColumnVector::add(int64_t value){
	if(writeIndex >= length){
		ensureSize(writeIndex * 2, true);
	}
	int index = writeIndex++;
	set(index, value);
}

void DateColumnVector::add(int value){
	if(writeIndex >= length){
		ensureSize(writeIndex * 2, true);
	}
	int index = writeIndex++;
	set(index, value);
}

void DateColumnVector::ensureSize(uint64_t size, bool preserveData){
	ColumnVector::ensureSize(size, preserveData);
	if(length < size){
		int *oldVector = dates;
		posix_memalign(reinterpret_cast<void **>(&dates), 32,
                           size * sizeof(int64_t));
		if(preserveData){
			std::copy(oldVector, oldVector + length, dates);
		}
		delete[] oldVector;
		memoryUsage += (long) sizeof(int) * (size - length);
		resize(size);
	}
}