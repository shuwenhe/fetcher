CXX = g++
CXXFLAGS = -std=c++17 -O2 -Wall
LIBS = -lclickhouse-cpp-lib -lcurl -lzstd -llz4 -lcityhash -lpthread

# 可执行文件
TARGETS = binance tushare

# 默认目标
all: $(TARGETS)

# binance 可执行文件
binance: binance.o
	$(CXX) $(CXXFLAGS) -o $@ binance.o $(LIBS)

# tushare 可执行文件
tushare: tushare.o
	$(CXX) $(CXXFLAGS) -o $@ tushare.o $(LIBS)

# 单独编译 binance.cpp
binance.o: binance.cpp
	$(CXX) $(CXXFLAGS) -c binance.cpp -o binance.o

# 单独编译 tushare.cc
tushare.o: tushare.cc
	$(CXX) $(CXXFLAGS) -c tushare.cc -o tushare.o

# 清理
clean:
	rm -f $(TARGETS) *.o

