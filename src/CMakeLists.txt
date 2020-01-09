add_library(libcsv
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/csv_reader.h
    csv_reader.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/csv_writer.h
    csv_writer.cpp)

target_include_directories(libcsv
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/../include)
