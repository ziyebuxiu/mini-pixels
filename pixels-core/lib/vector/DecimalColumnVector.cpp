//
// Created by yuly on 05.04.23.
//

#include "vector/DecimalColumnVector.h"
#include <iostream>
#include <sstream>
#include <cmath>
#include <string>
#include "duckdb/common/types/decimal.hpp"

/**
 * The decimal column vector with precision and scale.
 * The values of this column vector are the unscaled integer value
 * of the decimal. For example, the unscaled value of 3.14, which is
 * of the type decimal(3,2), is 314. While the precision and scale
 * of this decimal are 3 and 2, respectively.
 *
 * <p><b>Note: it only supports short decimals with max precision
 * and scale 18.</b></p>
 *
 * Created at: 05/03/2022
 * Author: hank
 */

DecimalColumnVector::DecimalColumnVector(int precision, int scale, bool encoding): ColumnVector(VectorizedRowBatch::DEFAULT_SIZE, encoding) {
    DecimalColumnVector(VectorizedRowBatch::DEFAULT_SIZE, precision, scale, encoding);
}

DecimalColumnVector::DecimalColumnVector(uint64_t len, int precision, int scale,
                                         bool encoding)
    : ColumnVector(len, encoding) {
    // decimal column vector has no encoding so we don't allocate memory to
    // this->vector
    this->vector = nullptr;
    this->precision = precision;
    this->scale = scale;

    using duckdb::Decimal;
    if (precision <= Decimal::MAX_WIDTH_INT16) {
        physical_type_ = PhysicalType::INT16;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int16_t));
        memoryUsage += (uint64_t)sizeof(int16_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT32) {
        physical_type_ = PhysicalType::INT32;
        posix_memalign(reinterpret_cast<void **>(&vector), 32,
                       len * sizeof(int32_t));
        memoryUsage += (uint64_t)sizeof(int32_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT64) {
        physical_type_ = PhysicalType::INT64;
        memoryUsage += (uint64_t)sizeof(uint64_t) * len;
    } else if (precision <= Decimal::MAX_WIDTH_INT128) {
        physical_type_ = PhysicalType::INT128;
        memoryUsage += (uint64_t)sizeof(uint64_t) * len;
    } else {
        throw std::runtime_error(
            "Decimal precision is bigger than the maximum supported width");
    }
}

void DecimalColumnVector::close() {
    if (!closed) {
        ColumnVector::close();
        if (physical_type_ == PhysicalType::INT16 ||
            physical_type_ == PhysicalType::INT32) {
            free(vector);
        }
        vector = nullptr;
    }
}

void DecimalColumnVector::print(int rowCount) {
//    throw InvalidArgumentException("not support print Decimalcolumnvector.");
    for(int i = 0; i < rowCount; i++) {
        std::cout<<vector[i]<<std::endl;
    }
}

DecimalColumnVector::~DecimalColumnVector() {
    if(!closed) {
        DecimalColumnVector::close();
    }
}

void * DecimalColumnVector::current() {
    if(vector == nullptr) {
        return nullptr;
    } else {
        return vector + readIndex;
    }
}

int DecimalColumnVector::getPrecision() {
	return precision;
}


int DecimalColumnVector::getScale() {
	return scale;
}


void DecimalColumnVector::add(std::string &value){
    std::istringstream ss(value);
    char dot;
    long intPart = 0, decimalPart = 0;
    if(dot != '.' || !(ss>>intPart>>dot>>decimalPart)){
        std::cerr << "Invalid decimal format!" << std::endl;
        return;
    }

    long tmp = decimalPart;
    long count1 = 0;
    while(tmp > 0){
        tmp /= 10;
        count1++;
    }

    intPart *= pow(10, count1);
    long val = (intPart > 0) ? (intPart + decimalPart) : (intPart - decimalPart);

    //adjust for scale
    while(count1 > scale + 1){
        val /= 10;
        count1--;
    }

    //round
    if(count1 > scale){
        long a = val % 10;
        val /= 10;
        if((val >0 && a >= 5) || (val < 0 && a <= -5))
            val += 1;
    }

    else if(count1 < scale){
        while(count1 < scale){
            val *= 10;
            count1++;
        }
    }

    if(writeIndex >= length)
        ensureSize(writeIndex * 2, true);

    vector[writeIndex] = val;
    isNull[writeIndex] = false;
    writeIndex++;
}

void DecimalColumnVector::add(int64_t value){
    // if(writeIndex >= length){
    //     ensureSize(writeIndex * 2, true);
    // }
    // int index = writeIndex;
    // vector[index] = value;
    // isNull[index] = false;
    throw std::invalid_argument("Invalid argument type");
}

void DecimalColumnVector::add(int value){
    // if(writeIndex >= length){
    //     ensureSize(writeIndex * 2, true);
    // }
    // int index = writeIndex;
    // vector[index] = value;
    // isNull[index] = false;
    throw std::invalid_argument("Invalid argument type");
}

void DecimalColumnVector::ensureSize(uint64_t size, bool preserveData){
	ColumnVector::ensureSize(size, preserveData);
	if(length < size){
		long *oldVector = vector;
		posix_memalign(reinterpret_cast<void **>(&vector), 32,
                           size * sizeof(int64_t));
		if(preserveData){
			std::copy(oldVector, oldVector + length, vector);
		}
		delete[] oldVector;
		memoryUsage += (uint64_t) sizeof(uint64_t) * (size - length); //能不能用long?
		resize(size);
	}
}