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
#include <numeric>
#include <string>

namespace csv
{

namespace detail
{
    struct shard_pos
    {
        std::string::size_type begin;
        std::string::size_type count;
    };

    std::vector<std::string> split(const std::string& line, char delimiter, const std::vector<size_t>& select_col)
    {
        const size_t num_shards = std::count(line.begin(), line.end(), delimiter) + 1;

        std::vector<shard_pos> shard_positions(num_shards);
        std::vector<std::string> shards;
        shards.reserve(select_col.size());

        std::string::size_type begin_idx = -1;
        for (size_t i = 0; i < num_shards; ++i)
        {
            const std::string::size_type end_idx = line.find_first_of(delimiter, begin_idx + 1);

            shard_positions[i].begin = begin_idx + 1;
            shard_positions[i].count = end_idx - begin_idx - 1;

            begin_idx = end_idx;
        }

        for (const size_t col : select_col)
        {
            const auto& pos = shard_positions[col];
            shards.emplace_back(line.substr(pos.begin, pos.count));
        }

        return shards;
    }

    std::vector<std::string> split(const std::string& line, char delimiter)
    {
        const size_t num_shards = std::count(line.begin(), line.end(), delimiter) + 1;
        std::vector<size_t> select_col(num_shards);

        size_t i = 0;
        std::generate(select_col.begin(), select_col.end(), [&i]() { return i++; } );

        return split(line, delimiter, select_col);
    }

    bool string_to_value(const std::string& str, char& val)
    {
        if (str.size() == 1)
        {
            val = str[0];
            return true;
        }

        return false;
    }

    bool string_to_value(const std::string& str, int& val)
    {
        char* end = nullptr;
        const auto tmp_val = std::strtol(str.c_str(), &end, 10);
        val = static_cast<int>(tmp_val);

        return (end == (str.c_str() + str.length())) && (errno != ERANGE);
    }

    bool string_to_value(const std::string& str, unsigned int& val)
    {
        char* end = nullptr;
        const auto tmp_val = std::strtoul(str.c_str(), &end, 10);
        val = static_cast<unsigned int>(tmp_val);

        return (end == (str.c_str() + str.length())) && (errno != ERANGE);
    }

    bool string_to_value(const std::string& str, long& val)
    {
        char* end = nullptr;
        val = std::strtol(str.c_str(), &end, 10);

        return (end == (str.c_str() + str.length())) && (errno != ERANGE);
    }

    bool string_to_value(const std::string& str, unsigned long& val)
    {
        char* end = nullptr;
        val = std::strtoul(str.c_str(), &end, 10);

        return (end == (str.c_str() + str.length())) && (errno != ERANGE);
    }

    bool string_to_value(const std::string& str, long long& val)
    {
        char* end = nullptr;
        val = std::strtoll(str.c_str(), &end, 10);

        return (end == (str.c_str() + str.length())) && (errno != ERANGE);
    }

    bool string_to_value(const std::string& str, unsigned long long& val)
    {
        char* end = nullptr;
        val = std::strtoull(str.c_str(), &end, 10);

        return (end == (str.c_str() + str.length())) && (errno != ERANGE);
    }

    bool string_to_value(const std::string& str, float& val)
    {
        char* end = nullptr;
        val = std::strtof(str.c_str(), &end);

        return (end == (str.c_str() + str.length())) && (errno != ERANGE);
    }

    bool string_to_value(const std::string& str, double& val)
    {
        char* end = nullptr;
        val = std::strtod(str.c_str(), &end);

        return (end == (str.c_str() + str.length())) && (errno != ERANGE);
    }

    bool string_to_value(const std::string& str, long double& val)
    {
        char* end = nullptr;
        val = std::strtold(str.c_str(), &end);

        return (end == (str.c_str() + str.length())) && (errno != ERANGE);
    }

    bool string_to_value(const std::string& str, std::string& val)
    {
        val = str;
        return true;
    }
} // namespace detail

reader::reader()
    : m_delimiter(',')
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

    return std::all_of(
            selected_cols.begin(),
            selected_cols.end(),
            [this](const std::string& col) { return select_next_col(col); });
}

bool reader::select_cols(std::vector<size_t> selected_cols)
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
        m_selected_cols = std::move(selected_cols);
    }

    return indexes_in_range;
}

bool reader::select_cols(const std::vector<bool>& selected_cols)
{
    if (!is_open())
    {
        return false;
    }

    if (selected_cols.size() > m_column_names.size())
    {
        return false;
    }

    m_selected_cols.clear();
    m_selected_cols.reserve(m_column_names.size());

    for (size_t i = 0; i < selected_cols.size(); ++i)
    {
        if (selected_cols[i])
        {
            m_selected_cols.emplace_back(i);
        }
    }

    return true;
}

bool reader::read_header()
{
    std::string header;
    if (std::getline(m_filestream, header))
    {
        m_column_names = detail::split(header, m_delimiter);
        return true;
    }

    return false;
}

bool reader::parse_next_line()
{
    std::string line;
    if (std::getline(m_filestream, line))
    {
        m_row.parse_line(line, m_delimiter, m_selected_cols);
        return true;
    }

    return false;
}

void reader::row::parse_line(
    const std::string& line,
    char delimiter,
    const std::vector<size_t>& selected_cols)
{
    m_column_values = detail::split(line, delimiter, selected_cols);
}

} // namespace csv
