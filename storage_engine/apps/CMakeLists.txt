project(emptyheaded)

file( GLOB APP_SOURCES RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.cpp )
foreach( testsourcefile ${APP_SOURCES} )
    string( REPLACE ".cpp" "" testname ${testsourcefile} )
    add_executable( ${testname} ${testsourcefile} )
    target_link_libraries( ${testname} emptyheaded )
    MESSAGE( STATUS "ADDED BINARY: " ${testname})
endforeach( testsourcefile ${APP_SOURCES} )
