# 编译器和选项
CXX = g++
CXXFLAGS = -std=c++17 -O2 -Wall
LIBS = -lclickhouse-cpp-lib -lcurl -lzstd -llz4 -lcityhash -lpthread

# 可执行文件
TARGETS = binance tushare okx

# 默认目标
all: $(TARGETS)

# --------------------------
# binance 可执行文件
binance: binance.o
	$(CXX) $(CXXFLAGS) -o $@ binance.o $(LIBS)

# tushare 可执行文件
tushare: tushare.o
	$(CXX) $(CXXFLAGS) -o $@ tushare.o $(LIBS)

# okx 可执行文件（需要 libwebsockets）
okx: okx.o
	$(CXX) $(CXXFLAGS) -o $@ okx.o $(LIBS) -lwebsockets

# --------------------------
# 单独编译源文件
binance.o: binance.cpp
	$(CXX) $(CXXFLAGS) -c binance.cpp -o binance.o

tushare.o: tushare.cc
	$(CXX) $(CXXFLAGS) -c tushare.cc -o tushare.o

okx.o: okx.cc
	$(CXX) $(CXXFLAGS) -c okx.cc -o okx.o

# --------------------------
# 清理
clean:
	rm -f $(TARGETS) *.o

