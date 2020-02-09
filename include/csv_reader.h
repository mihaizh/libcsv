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
#ifndef CSV_READER_H
#define CSV_READER_H

#include <algorithm>
#include <numeric>
#include <fstream>
#include <vector>
#include <cassert>
#include <sstream>

namespace csv
{

namespace detail
{
    std::vector<std::streamoff> get_offsets(const std::string& line, char delimiter);
} // namespace detail

class reader
{
public:
    class row
    {
    public:
        row();
        row(const row&) = default;
        row(row&&) = default;
        row& operator=(const row&) = default;
        row& operator=(row&&) = default;
        ~row() = default;

        void parse_line(std::string line, char delimiter = ',');
        bool parse_line(std::ifstream& filestream, char delimiter = ',');

        template <typename... Args>
        bool read(Args&... args) const;

        template <typename... Args>
        bool read_cols(const std::vector<bool>& cols, Args&... args) const;

        template <typename Arg>
        Arg get(size_t index) const;
        template <typename Arg>
        bool get(size_t index, Arg& arg) const;

        size_t size() const
        {
            return m_column_offsets.size();
        }

        const std::string& get_line()
        {
            return m_line;
        }

    private:
        void parse_line_impl(char delimiter);

        template <typename Arg>
        bool read_impl(const std::vector<bool>& cols, size_t idx, Arg& arg) const;
        template <typename Arg, typename... Args>
        bool read_impl(const std::vector<bool>& cols, size_t idx, Arg& arg, Args&... args) const;
        template <typename Arg>
        bool read_next_col(const std::vector<bool>& cols, size_t& idx, Arg& arg) const;

        std::string m_line;
        mutable std::istringstream m_line_stream;

        std::vector<std::streamoff> m_column_offsets;
        std::vector<bool> m_default_selected_cols;

        friend class reader;
    };

    reader();
    reader(const reader&) = delete;
    reader(reader&&) = default;
    reader& operator=(const reader&) = delete;
    reader& operator=(reader&&) = default;
    ~reader() = default;

    bool open(const char* filename, char delimiter = ',');
    bool open(const std::string& filename, char delimiter = ',')
    {
        return open(filename.c_str(), delimiter);
    }

    bool is_open() const
    {
        return m_filestream.is_open();
    }

    char get_delimiter() const
    {
        return m_delimiter;
    }

    const std::vector<std::string>& get_column_names() const
    {
        return m_column_names;
    }

    size_t get_column_index(const std::string& name) const
    {
        return get_column_index(name.c_str());
    }

    const row& get_row() const
    {
        return m_row;
    }

    size_t get_column_index(const char* name) const;

    bool next_row();

    template <typename... Args>
    bool read_row(Args&... args);

    template <typename... Args>
    bool select_cols(const Args&... args);
    bool select_cols(const std::vector<std::string>& selected_cols);
    bool select_cols(const std::vector<size_t>& selected_cols);
    bool select_cols(std::vector<bool> selected_cols);

private:
    template <typename Arg>
    bool select_cols_impl(const Arg& arg);
    template <typename Arg, typename... Args>
    bool select_cols_impl(const Arg& arg, const Args&... args);
    template <typename Arg>
    bool select_next_col(const Arg& arg);

    bool read_header();
    bool parse_next_line();

    std::ifstream m_filestream;
    char m_delimiter;

    size_t m_selected_cols_num;
    std::vector<bool> m_selected_cols;
    std::vector<std::string> m_column_names;

    row m_row;
};

template <typename... Args>
bool reader::row::read(Args&... args) const
{
    return read_cols(m_default_selected_cols, args...);
}

template <typename... Args>
bool reader::row::read_cols(const std::vector<bool>& cols, Args&... args) const
{
    if (sizeof...(args) > m_column_offsets.size())
    {
        return false;
    }

    if (cols.size() != m_column_offsets.size())
    {
        return false;
    }

    size_t idx = 0;
    return read_impl(cols, idx, args...);
}

template <typename Arg>
bool reader::row::read_impl(const std::vector<bool>& cols, size_t idx, Arg& arg) const
{
    return read_next_col(cols, idx, arg);
}

template <typename Arg, typename... Args>
bool reader::row::read_impl(const std::vector<bool>& cols, size_t idx, Arg& arg, Args&... args) const
{
    if (read_next_col(cols, idx, arg))
    {
        return read_impl(cols, idx, args...);
    }

    return false;
}

template <typename Arg>
bool reader::row::read_next_col(const std::vector<bool>& cols, size_t& idx, Arg& arg) const
{
    while ((idx < cols.size()) && !cols[idx])
    {
        ++idx;
    }

    if (idx >= m_column_offsets.size())
    {
        return false;
    }

    m_line_stream.seekg(m_column_offsets[idx++]);
    m_line_stream >> arg;
    return true;
}

template <typename Arg>
Arg reader::row::get(size_t index) const
{
    Arg val;
    const bool ret = get(index, val);
    assert(ret);

    return val;
}

template <typename Arg>
bool reader::row::get(size_t index, Arg& arg) const
{
    if (index < m_column_offsets.size())
    {
        m_line_stream.seekg(m_column_offsets[index]);
        m_line_stream >> arg;
        return true;
    }

    return false;
}

template <typename... Args>
bool reader::read_row(Args&... args)
{
    if (sizeof...(args) != m_selected_cols_num)
    {
        return false;
    }

    if (!is_open() || m_filestream.eof())
    {
        return false;
    }

    if (!parse_next_line())
    {
        return false;
    }

    return m_row.read_cols(m_selected_cols, args...);
}

template <typename... Args>
bool reader::select_cols(const Args&... args)
{
    if (!is_open())
    {
        return false;
    }

    std::fill(m_selected_cols.begin(), m_selected_cols.end(), false);
    const bool cols_selected = select_cols_impl(args...);

    m_selected_cols_num = std::accumulate(m_selected_cols.begin(), m_selected_cols.end(), size_t(0));

    return cols_selected;
}

template <typename Arg>
bool reader::select_cols_impl(const Arg& arg)
{
    return select_next_col(arg);
}

template <typename Arg, typename... Args>
bool reader::select_cols_impl(const Arg& arg, const Args&... args)
{
    if (!select_next_col(arg))
    {
        return false;
    }

    return select_cols_impl(args...);
}

template <typename Arg>
bool reader::select_next_col(const Arg& arg)
{
    const auto it = std::find(m_column_names.begin(), m_column_names.end(), arg);
    if (it == m_column_names.end())
    {
        return false;
    }

    m_selected_cols[std::distance(m_column_names.begin(), it)] = true;
    return true;
}

} // namespace csv

#endif // CSV_READER_H