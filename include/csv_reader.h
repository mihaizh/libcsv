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
#include <fstream>
#include <vector>
#include <memory>
#include <cassert>

namespace csv
{

namespace detail
{
    bool string_to_value(const std::string& str, char& val);
    bool string_to_value(const std::string& str, int& val);
    bool string_to_value(const std::string& str, unsigned int& val);
    bool string_to_value(const std::string& str, long& val);
    bool string_to_value(const std::string& str, unsigned long& val);
    bool string_to_value(const std::string& str, long long& val);
    bool string_to_value(const std::string& str, unsigned long long& val);
    bool string_to_value(const std::string& str, float& val);
    bool string_to_value(const std::string& str, double& val);
    bool string_to_value(const std::string& str, long double& val);
    bool string_to_value(const std::string& str, std::string& val);
} // namespace detail

class reader
{
public:
    class row
    {
    public:
        row() = default;
        row(const row&) = default;
        row(row&&) = default;
        row& operator=(const row&) = default;
        row& operator=(row&&) = default;
        ~row() = default;

        template <typename... Args>
        bool read(Args&... args);

        template <typename Arg>
        Arg get(size_t index);

    private:
        void parse_line(
                const std::string& line,
                char delimiter,
                const std::vector<size_t>& selected_cols);

        template <typename Arg>
        bool read_impl(const std::vector<std::string>& cols, size_t idx, Arg& arg);
        template <typename Arg, typename... Args>
        bool read_impl(const std::vector<std::string>& cols, size_t idx, Arg& arg, Args&... args);
        template <typename Arg>
        bool read_next_col(const std::vector<std::string>& cols, size_t& idx, Arg& arg);

        std::vector<std::string> m_column_values;

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
    bool select_cols(std::vector<size_t> selected_cols);
    bool select_cols(const std::vector<bool>& selected_cols);

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

    std::vector<size_t> m_selected_cols;
    std::vector<std::string> m_column_names;

    row m_row;
};

template <typename... Args>
bool reader::row::read(Args&... args)
{
    if (sizeof...(args) > m_column_values.size())
    {
        return false;
    }

    size_t idx = 0;
    return read_impl(m_column_values, idx, args...);
}

template <typename Arg>
bool reader::row::read_impl(const std::vector<std::string>& cols, size_t idx, Arg& arg)
{
    return read_next_col(cols, idx, arg);
}

template <typename Arg, typename... Args>
bool reader::row::read_impl(const std::vector<std::string>& cols, size_t idx, Arg& arg, Args&... args)
{
    if (read_next_col(cols, idx, arg))
    {
        return read_impl(cols, idx, args...);
    }

    return false;
}

template <typename Arg>
bool reader::row::read_next_col(const std::vector<std::string>& cols, size_t& idx, Arg& arg)
{
    return detail::string_to_value(cols[idx++], arg);
}

template <typename Arg>
Arg reader::row::get(size_t index)
{
    assert(index < m_column_values.size());
    Arg val;
    assert(detail::string_to_value(m_column_values[index], val));
    return val;
}

template <typename... Args>
bool reader::read_row(Args&... args)
{
    if (sizeof...(args) != m_selected_cols.size())
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

    return m_row.read(args...);
}

template <typename... Args>
bool reader::select_cols(const Args&... args)
{
    if (!is_open())
    {
        return false;
    }

    m_selected_cols.clear();
    m_selected_cols.reserve(sizeof...(args));

    return select_cols_impl(args...);
}

template <typename Arg>
bool reader::select_cols_impl(const Arg& arg)
{
    select_next_col(arg);
}

template <typename Arg, typename... Args>
bool reader::select_cols_impl(const Arg& arg, const Args&... args)
{
    select_next_col(arg);
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

    m_selected_cols.emplace_back(std::distance(m_column_names.begin(), it));
    return true;
}

} // namespace csv

#endif // CSV_READER_H