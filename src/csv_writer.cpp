/*
 * MIT License
 *
 * Copyright (c) 2020 Zaha Mihai
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
#include "csv_writer.h"
#include <string>

namespace csv
{

writer::row::row(std::ofstream& filestream)
    : m_filestream(filestream)
{
}

writer::row::~row()
{
    flush();
}

void writer::row::flush()
{
    const std::streamoff offset = (m_actual_columns > 0);
    m_filestream.get().seekp(m_filestream.get().tellp() - offset);

    m_filestream.get() << '\n';
    m_actual_columns = 0;
}

writer::writer()
    : m_delimiter(','),
      m_header_written(false)
{
}

writer::~writer()
{
    m_filestream.flush();
}

bool writer::open(const char* filename, char delimiter)
{
    m_header_written = false;
    m_delimiter = delimiter;

    m_filestream.open(filename);
    m_filestream.imbue(std::locale{ "en_US.UTF8" });

    return is_open();
}

void writer::write_header()
{
    m_filestream << m_column_names[0];
    for (size_t i = 1; i < m_column_names.size(); ++i)
    {
        m_filestream << ',' << m_column_names[i];
    }
    m_filestream << std::endl;
    m_header_written = true;
}

writer::row writer::new_row()
{
    if (!m_header_written)
    {
        write_header();
    }

    return row(m_filestream);
}

} // namespace csv
