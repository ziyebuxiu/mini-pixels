/*
 * Copyright 2024 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

#include "writer/TimestampColumnWriter.h"
#include "utils/BitUtils.h"

TimestampColumnWriter::TimestampColumnWriter(std::shared_ptr<TypeDescription> type, std::shared_ptr<PixelsWriterOption> writerOption)
    : ColumnWriter(type, writerOption), curPixelVector(pixelStride)
{
    runlengthEncoding = encodingLevel.ge(EncodingLevel::Level::EL2);
    if (runlengthEncoding)
    {
        encoder = std::make_unique<RunLenIntEncoder>(1, 1);
    }
}

int TimestampColumnWriter::write(std::shared_ptr<ColumnVector> vector, int size)
{
    auto columnVector = std::static_pointer_cast<TimestampColumnVector>(vector);
    if (!columnVector)
    {
        throw std::invalid_argument("Invalid vector type");
    }
    long *timestamps = columnVector->times;
    int curPartLength;         // size of the partition which belongs to current pixel
    int curPartOffset = 0;     // starting offset of the partition which belongs to current pixel
    int nextPartLength = size; // size of the partition which belongs to next pixel
    while ((curPixelIsNullIndex + nextPartLength) >= pixelStride)
    {
        curPartLength = pixelStride - curPixelIsNullIndex;
        writeCurPartTimestamp(columnVector, timestamps, curPartLength, curPartOffset);
        newPixel();
        curPartOffset += curPartLength;
        nextPartLength = size - curPartOffset;
    }
    return outputStream->getWritePos();
}

void TimestampColumnWriter::close()
{
    if (runlengthEncoding && encoder)
    {
        encoder->clear();
    }
    ColumnWriter::close();
}

void TimestampColumnWriter::writeCurPartTimestamp(std::shared_ptr<ColumnVector> columnVector, long *values, int curPartLength, int curPartOffset)
{
    for (int i = 0; i < curPartLength; i++)
    {
        if (columnVector->isNull[i + curPartOffset])
        {
            curPixelVector[curPixelVectorIndex++] = 0;
        }
        else
        {
            curPixelVector[curPixelVectorIndex++] = values[i + curPartOffset];
        }
    }
    std::copy(columnVector->isNull + curPartOffset, columnVector->isNull + curPartOffset + curPartLength, isNull.begin() + curPixelIsNullIndex);
    curPixelIsNullIndex += curPartLength;
}


void TimestampColumnWriter::newPixel()
{
    if (runlengthEncoding)
    {
        std::vector<byte> buffer(curPixelVectorIndex * sizeof(int));
        int resLen;
        encoder->encode(curPixelVector.data(), buffer.data(), curPixelVectorIndex, resLen);
        outputStream->putBytes(buffer.data(), resLen);
    }
    else
    {
        std::cout << "no runlength encoding" << std::endl;
        std::shared_ptr<ByteBuffer> curVecPartitionBuffer;
        EncodingUtils encodingUtils;
        curVecPartitionBuffer = std::make_shared<ByteBuffer>(curPixelVectorIndex * sizeof(int));
        if (byteOrder == ByteOrder::PIXELS_LITTLE_ENDIAN)
        {
            for (int i = 0; i < curPixelVectorIndex; i++)
            {
                encodingUtils.writeIntLE(curVecPartitionBuffer, curPixelVector[i]);
            }
        }
        else
        {
            for (int i = 0; i < curPixelVectorIndex; i++)
            {
                encodingUtils.writeIntBE(curVecPartitionBuffer, curPixelVector[i]);
            }
        }
        outputStream->putBytes(curVecPartitionBuffer->getBuffer(), curVecPartitionBuffer->size());
    }

    ColumnWriter::newPixel();
}

pixels::proto::ColumnEncoding TimestampColumnWriter::getColumnChunkEncoding()
{
    pixels::proto::ColumnEncoding columnEncoding;
    if (runlengthEncoding)
    {
        columnEncoding.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_RUNLENGTH);
    }
    else
    {
        columnEncoding.set_kind(pixels::proto::ColumnEncoding::Kind::ColumnEncoding_Kind_NONE);
    }
    return columnEncoding;
}


bool TimestampColumnWriter::decideNullsPadding(std::shared_ptr<PixelsWriterOption> writerOption)
{
    if (writerOption->getEncodingLevel().ge(EncodingLevel::Level::EL2))
    {
        return false;
    }
    return writerOption->isNullsPadding();
}