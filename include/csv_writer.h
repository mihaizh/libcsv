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

#ifndef CSV_WRITER_H
#define CSV_WRITER_H

#include <fstream>
#include <vector>
#include <functional>

namespace csv
{

class writer
{
public:
    class row
    {
    public:
        explicit row(std::ofstream& filestream);
        row(const row&) = delete;
        row(row&&) = default;
        row& operator=(const row&) = delete;
        row& operator=(row&&) = default;
        ~row();

        size_t get_columns() const
        {
            return m_actual_columns;
        }

        template <typename Arg>
        void write_column(const Arg& arg);
        template <typename Arg, typename... Args>
        void write_columns(const Arg& arg, const Args&... args);

        void flush();

    private:
        template <typename Arg>
        void write_columns(const Arg& arg);

        std::reference_wrapper<std::ofstream> m_filestream;
        size_t m_actual_columns = 0;

        friend class writer;
    };

    writer();
    writer(const writer&) = delete;
    writer(writer&&) = default;
    writer& operator=(const writer&) = delete;
    writer& operator=(writer&&) = default;
    ~writer();

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

    template <typename... Args>
    void set_column_names(const Args&... args);
    void set_column_names(std::vector<std::string> column_names)
    {
        m_column_names = std::move(column_names);
    }

    template <typename... Args>
    bool write_row(const Args&... args);
    row new_row();

private:
    template <typename Arg>
    void set_col_name_impl(const Arg& arg);
    template <typename Arg, typename... Args>
    void set_col_name_impl(const Arg& arg, const Args&... args);

    template <typename Arg>
    void write_row_impl(const Arg& arg);
    template <typename Arg, typename... Args>
    void write_row_impl(const Arg& arg, const Args&... args);

    void write_header();

    std::ofstream m_filestream;
    char m_delimiter;

    bool m_header_written;
    std::vector<std::string> m_column_names;
};

template <typename Arg>
void writer::row::write_column(const Arg& arg)
{
    write_columns(arg);
}

template <typename Arg, typename... Args>
void writer::row::write_columns(const Arg& arg, const Args&... args)
{
    write_columns(arg);
    write_columns(args...);
}

template <typename Arg>
void writer::row::write_columns(const Arg &arg)
{
    m_filestream.get() << arg << ',';
    ++m_actual_columns;
}


template <typename... Args>
void writer::set_column_names(const Args&... args)
{
    m_column_names.clear();
    m_column_names.reserve(sizeof...(args));
    set_col_name_impl(args...);
}

template <typename Arg>
void writer::set_col_name_impl(const Arg& arg)
{
    m_column_names.emplace_back(arg);
}

template <typename Arg, typename... Args>
void writer::set_col_name_impl(const Arg& arg, const Args&... args)
{
    m_column_names.emplace_back(arg);
    set_col_name_impl(args...);
}

template <typename... Args>
bool writer::write_row(const Args&... args)
{
    if (!is_open() || m_column_names.empty())
    {
        return false;
    }

    if (!m_header_written)
    {
        write_header();
    }

    if (sizeof...(args) != m_column_names.size())
    {
        return false;
    }

    write_row_impl(args...);
    m_filestream << std::endl;

    return true;
}

template <typename Arg>
void writer::write_row_impl(const Arg& arg)
{
    m_filestream << arg;
}

template <typename Arg, typename... Args>
void writer::write_row_impl(const Arg& arg, const Args&... args)
{
    m_filestream << arg << ',';
    write_row_impl(args...);
}

} // namespace csv

#endif // CSV_WRITER_H