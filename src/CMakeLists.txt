add_library(libcsv
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/csv_reader.h
    csv_reader.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../include/csv_writer.h
    csv_writer.cpp)

target_include_directories(libcsv
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/../include)

set_target_properties(libcsv
    PROPERTIES
        CXX_STANDARD 11
        CXX_STANDARD_REQUIRED YES
        CXX_EXTENSIONS NO)
