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
#include "csv_reader.h"
#include <string>

namespace csv
{

namespace detail
{
    std::vector<std::streamoff> get_offsets(const std::string& line, char delimiter)
    {
        const size_t num_shards = std::count(line.begin(), line.end(), delimiter) + 1;

        std::vector<std::streamoff> stream_offsets(num_shards);
        std::string::size_type begin_idx = -1;
        for (size_t i = 0; i < num_shards; ++i)
        {
            const std::string::size_type end_idx = line.find_first_of(delimiter, begin_idx + 1);

            stream_offsets[i] = begin_idx + 1;

            begin_idx = end_idx;
        }

        return stream_offsets;
    }
} // namespace detail

void reader::row::parse_line(std::string line, char delimiter)
{
    m_line = std::move(line);
    m_column_offsets = detail::get_offsets(m_line, delimiter);

    std::replace(m_line.begin(), m_line.end(), delimiter, '\n');
    m_line_stream.str(m_line);

    m_default_selected_cols.resize(m_column_offsets.size());
    std::fill(m_default_selected_cols.begin(), m_default_selected_cols.end(), true);
}

reader::reader()
    : m_delimiter(','),
      m_selected_cols_num(0)
{
}

bool reader::open(const char* filename, char delimiter)
{
    m_filestream.open(filename);
    m_delimiter = delimiter;

    return is_open() && read_header() && select_cols(m_column_names);
}

bool reader::next_row()
{
    return parse_next_line();
}

size_t reader::get_column_index(const char* name) const
{
    const auto it = std::find(m_column_names.begin(), m_column_names.end(), name);
    return (it == m_column_names.end())
        ? size_t(-1)
        : std::distance(m_column_names.begin(), it);
}

bool reader::select_cols(const std::vector<std::string>& selected_cols)
{
    if (!is_open())
    {
        return false;
    }

    std::fill(m_selected_cols.begin(), m_selected_cols.end(), false);

    const bool cols_selected = std::all_of(
            selected_cols.begin(),
            selected_cols.end(),
            [this](const std::string& col) { return select_next_col(col); });

    if (!cols_selected)
    {
        std::fill(m_selected_cols.begin(), m_selected_cols.end(), true);
    }

    m_selected_cols_num = std::accumulate(m_selected_cols.begin(), m_selected_cols.end(), size_t(0));
    return cols_selected;
}

bool reader::select_cols(const std::vector<size_t>& selected_cols)
{
    if (!is_open())
    {
        return false;
    }

    const bool indexes_in_range = std::all_of(
            selected_cols.begin(),
            selected_cols.end(),
            [this](size_t idx) { return idx < m_column_names.size(); });

    if (indexes_in_range)
    {
        std::fill(m_selected_cols.begin(), m_selected_cols.end(), false);
        for (auto idx : selected_cols)
        {
            m_selected_cols[idx] = true;
        }
    }

    m_selected_cols_num = std::accumulate(m_selected_cols.begin(), m_selected_cols.end(), size_t(0));
    return indexes_in_range;
}

bool reader::select_cols(std::vector<bool> selected_cols)
{
    if (!is_open())
    {
        return false;
    }

    if (selected_cols.size() != m_column_names.size())
    {
        return false;
    }

    m_selected_cols = std::move(selected_cols);
    m_selected_cols_num = std::accumulate(m_selected_cols.begin(), m_selected_cols.end(), size_t(0));
    return true;
}

bool reader::read_header()
{
    std::string header;
    if (std::getline(m_filestream, header))
    {
        const auto num_cols = std::count(header.begin(), header.end(), m_delimiter) + 1;
        m_column_names.resize(num_cols);

        std::replace(header.begin(), header.end(), m_delimiter, '\n');
        std::stringstream line_stream(header);

        for (size_t i = 0; line_stream >> m_column_names[i]; ++i) {}

        m_selected_cols.resize(num_cols);
        std::fill(m_selected_cols.begin(), m_selected_cols.end(), true);
        m_selected_cols_num = m_selected_cols.size();
        return true;
    }

    return false;
}

bool reader::parse_next_line()
{
    std::string line;
    if (std::getline(m_filestream, line))
    {
        m_row.parse_line(std::move(line), m_delimiter);
        return true;
    }

    return false;
}

} // namespace csv
