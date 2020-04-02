// hevc_rtmp_test.cpp : This file contains the 'main' function. Program execution begins and ends there.
//
#include <winsock2.h>
#include <iostream>
#include <vector>
#include <thread>
#include <string>
#include <queue>
#include <mutex>
#include "TCP.h"
#include "Log.h"
#include "Packet.h"
extern "C" {
#include "libavformat/avformat.h"
#include "libavcodec/avcodec.h"
}

// win socket
#pragma comment(lib, "ws2_32.lib")
// rtmpdump
//#pragma comment(lib, "rtmp/librtmp.lib")
// openssl
#pragma comment(lib, "openssl/libeay32.lib")
#pragma comment(lib, "openssl/ssleay32.lib")
// ffmpeg
#pragma comment(lib, "ffmpeg/avformat.lib")
#pragma comment(lib, "ffmpeg/avcodec.lib")
#pragma comment(lib, "ffmpeg/avutil.lib")

//#pragma comment(linker, "/SUBSYSTEM:windows /ENTRY:mainCRTStartup")

#define STREAM_CHANNEL_METADATA  0x03
#define STREAM_CHANNEL_VIDEO     0x04
#define STREAM_CHANNEL_AUDIO     0x05

#define KB(bytes) (bytes/1024.0)
#define MB(bytes) (KB(bytes)/1024.0)
#define GB(bytes) (MB(bytes)/1024.0)

#define BUFSIZE 10 * 1024
#define SERVER_IP "192.168.1.104"
#define SERVER_PORT 5566

enum
{
	STREAMING_START,
	STREAMING_IN_PROGRESS,
	STREAMING_STOPPING,
	STREAMING_STOPPED
};

struct Frame
{
	int64_t timestamp;
	int32_t size;
	char *data;
};

typedef std::queue<Frame> FrameData;
struct StreamInfo
{
	std::string app;
	int timebase;
	FrameData frameData;
};

struct StatisticInfo
{
	int64_t totalRecvPackets;
	int64_t totalRecvFrames;
	int64_t totalRecvBytes;
	int64_t totalWriteFrames;
	int64_t totalWriteBytes;
	int64_t recvBitrate;
	std::mutex mux;
};

struct STREAMING_CLIENT
{
	TCP conn;
	int state;
	StreamInfo stream;
	std::string filePath;
	std::mutex mux;
	StatisticInfo stat;
};

bool init_sockets( )
{
#ifdef WIN32
	WORD version = MAKEWORD( 1, 1 );
	WSADATA wsaData;
	return ( WSAStartup( version, &wsaData ) == 0 );
#endif
	return true;
}

void cleanup_sockets( )
{
#ifdef WIN32
	WSACleanup( );
#endif
}

void
stopStreaming( STREAMING_CLIENT * client )
{
	if ( client->state != STREAMING_STOPPED )
	{
		if ( client->state == STREAMING_IN_PROGRESS )
		{
			client->state = STREAMING_STOPPING;

			// wait for streaming threads to exit
			while ( client->state != STREAMING_STOPPED )
				Sleep( 10 );
		}
		client->state = STREAMING_STOPPED;
	}
}


std::mutex mux;
std::condition_variable cond;
int thread_func_for_receiver( void *arg )
{
	RTMP_Log( RTMP_LOGDEBUG, "receiver thread is start..." );
	STREAMING_CLIENT* client = ( STREAMING_CLIENT* ) arg;
	StreamInfo& stream = client->stream;
	size_t timebase = stream.timebase;

	while (true )
	{
		if ( send_play_packet( client->conn, 
							   get_current_milli(), 
							   client->stream.app.c_str() ) <= 0 )
		{
			Sleep( 100 );
			continue;
		}
		Sleep( 10 );	// wait for packet comming

		// recv ack
		PACKET pkt;
		if ( recv_packet( client->conn, pkt, NonBlocking ) <= 0 )
		{
			Sleep( 10 );
			continue;
		}

		if ( pkt.header.type == Ack )
		{
			client->stream.timebase = pkt.header.reserved;
			cond.notify_one( );
			break;
		}
		Sleep( 100 );
	}

#ifdef _DEBUG
	int64_t timeBeg = time( 0 );
	int64_t lastTimestamp = 0;
#endif // _DEBUG
	int32_t maxRecvBuf = SEND_BUF_SIZE;
	auto cmp = [ ] ( PACKET& a, PACKET& b ) { return a.header.seq > b.header.seq; };
	std::priority_queue<PACKET, std::vector<PACKET>, decltype( cmp )> priq( cmp );
	Frame frame;
	ZeroMemory( &frame, sizeof Frame );
	int totalSize = 0;
	size_t bodySize = 0;
	while ( client->state == STREAMING_START )
	{
		// receive packet
		PACKET pkt;
		int MP = 0;
		int recvBytes = 0;
		do
		{
			if ( (recvBytes = recv_packet( client->conn, pkt, NonBlocking )) <= 0 ) // no packet, continue loop next
				break;
#ifdef _DEBUG
			if (recvBytes > MAX_PACKET_SIZE )
				break;
			int64_t currentTime = time(0);
			client->stat.totalRecvBytes += BODY_SIZE_H(pkt.header);
			client->stat.totalRecvPackets++;
			if ( currentTime != timeBeg )
				client->stat.recvBitrate = client->stat.totalRecvBytes / ( currentTime - timeBeg );
			if ( lastTimestamp == 0 )
				lastTimestamp = pkt.header.timestamp;
			else
			{
				if ( lastTimestamp != pkt.header.timestamp )
				{
					client->stat.totalRecvFrames++;
					lastTimestamp = pkt.header.timestamp;
				}
			}
#endif // _DEBUG

			if ( maxRecvBuf < pkt.header.size )
			{
				maxRecvBuf = ( pkt.header.size + MAX_PACKET_SIZE - 1 ) / MAX_PACKET_SIZE * MAX_PACKET_SIZE;
				client->conn.set_socket_rcvbuf_size( maxRecvBuf );
			}

			MP = pkt.header.MP;
			switch ( pkt.header.type )
			{
			case Pull:
			{
				if ( stream.app != pkt.header.app )
				{
					send_err_packet( client->conn,
									 get_current_milli( ),
									 pkt.header.app );
				}

				if ( priq.empty( ) )
				{
					frame.timestamp = pkt.header.timestamp;
					frame.size = pkt.header.size;
					frame.data = ( char * ) malloc( frame.size );
				}

				if (frame.timestamp == pkt.header.timestamp )
				{
					priq.push( pkt );
					totalSize += BODY_SIZE_H( pkt.header );
				}
				else
					break;

				// full frame
				if ( totalSize == frame.size )
				{
					PACKET tmpPack;
					int idx = 0;
					while ( !priq.empty( ) )
					{
						tmpPack = priq.top( );
						priq.pop( );
						bodySize = BODY_SIZE_H( tmpPack.header );
						memcpy( &frame.data[ idx ], tmpPack.body, bodySize );
						idx += bodySize;
					}
					std::unique_lock<std::mutex> lock( client->mux );
					stream.frameData.push( frame );
					lock.unlock( );
					totalSize = 0;
				}
				break;
			}
			case Fin:
			{
				if ( stream.app != pkt.header.app )
				{
					send_err_packet( client->conn,
									 get_current_milli( ),
									 pkt.header.app );
				}
				stopStreaming( client );
				break;
			}
// 			case Ack:
// 			{
// 				break;
// 			}
// 			case Err:
// 			{
// 				break;
// 			}
			default:
				RTMP_Log( RTMP_LOGDEBUG, "unknown packet." );
				break;
			}
		} while (MP);

		Sleep( 10 );
	}
	RTMP_Log( RTMP_LOGDEBUG, "receiver thread is quit." );
	return true;
}

int thread_func_for_writer( void *arg )
{
	RTMP_LogPrintf( "writer thread is start...\n" );
	STREAMING_CLIENT *client = ( STREAMING_CLIENT * ) arg;
	
	FILE *fp = fopen( client->filePath.c_str(), "wb" );
	if ( !fp )
	{
		RTMP_LogPrintf( "Open File Error.\n" );
		fclose( fp );
		return -1;
	}

	std::unique_lock<std::mutex> locker( mux );
	cond.wait( locker );

	FrameData& frameData = client->stream.frameData;
	int timebase = client->stream.timebase;
	int64_t currentTime = 0, waitTime = 0;
	while ( client->state == STREAMING_START )
	{
		if ( frameData.empty( ) )
		{
			Sleep( 5 );
			continue;
		}
		std::unique_lock<std::mutex> lock( client->mux );
		Frame frame = frameData.front( );
		frameData.pop( );
		lock.unlock( );

		currentTime = get_current_milli( );
		waitTime = frame.timestamp - currentTime;
		if ( waitTime >= 0 )
			Sleep( waitTime );
		fwrite( frame.data, 1, frame.size, fp );
#ifdef _DEBUG
		int64_t writeTimestamp = get_current_milli( );
		RTMP_Log( RTMP_LOGDEBUG, "write frame %dB, frame timestamp=%lld, write timestamp=%lld, W-F=%lld",
				  frame.size,
				  frame.timestamp,
				  writeTimestamp,
				  writeTimestamp - frame.timestamp );
		client->stat.totalWriteBytes += frame.size;
		client->stat.totalWriteFrames++;
#endif // _DEBUG
		free( frame.data );
	}

	fclose( fp );
	RTMP_LogPrintf( "writer thread is quit.\n" );
	return true;
}

void show_statistics(STREAMING_CLIENT* client )
{
	RTMP_LogAndPrintf( RTMP_LOGDEBUG, "recv %lldB, %lld packets, %lld frames, %.2fKB/s\nwrite %lldB, %lld frames",
					   client->stat.totalRecvBytes,
					   client->stat.totalRecvPackets,
					   client->stat.totalRecvFrames,
					   KB(client->stat.recvBitrate),
					   client->stat.totalWriteBytes,
					   client->stat.totalWriteFrames
					   );
}

int thread_func_for_controller( void *arg )
{
	RTMP_LogPrintf( "controller thread is start...\n" );
	STREAMING_CLIENT *client = ( STREAMING_CLIENT * ) arg;
	std::string choice;
	while ( client->state == STREAMING_START )
	{
		std::cin >> choice;
		if ( choice == "quit" || choice == "q" || choice == "exit" )
		{
			RTMP_LogAndPrintf( RTMP_LOGDEBUG, "Exiting" );
			stopStreaming( client );
		}
		else if ( choice == "status" || choice == "s" )
		{
			show_statistics( client );
		}
	}
	RTMP_LogPrintf( "controller thread is quit.\n" );
	return 0;
}

int thread_func_for_aliver( void *arg )
{
	RTMP_Log( RTMP_LOGDEBUG, "cleaner thread is start..." );
	STREAMING_CLIENT* client = ( STREAMING_CLIENT* ) arg;
	StreamInfo& streamInfo = client->stream;
	while ( client->state == STREAMING_START )
	{
		// send heart packet
		send_alive_packet( client->conn, 
						   get_current_milli( ), 
						   streamInfo.app.c_str( ) );
		Sleep( 1000 );
	}
	RTMP_Log( RTMP_LOGDEBUG, "cleaner thread is quit." );
	return true;
}

int main( )
{
#ifdef _DEBUG
	FILE* dumpfile = fopen( "hevc_client.dump", "a+" );
	RTMP_LogSetOutput( dumpfile );
	RTMP_LogSetLevel( RTMP_LOGALL );
	RTMP_LogThreadStart( );

	SYSTEMTIME tm;
	GetSystemTime( &tm );
	RTMP_Log( RTMP_LOGDEBUG, "==============================" );
	RTMP_Log( RTMP_LOGDEBUG, "log file:\thevc_client.dump" );
	RTMP_Log( RTMP_LOGDEBUG, "log timestamp:\t%lld", get_current_milli( ) );
	RTMP_Log( RTMP_LOGDEBUG, "log date:\t%d-%d-%d %d:%d:%d.%d",
			  tm.wYear,
			  tm.wMonth,
			  tm.wDay,
			  tm.wHour + 8, tm.wMinute, tm.wSecond, tm.wMilliseconds );
	RTMP_Log( RTMP_LOGDEBUG, "==============================" );
#endif
	init_sockets( );

	STREAMING_CLIENT *client = new STREAMING_CLIENT;
	client->state = STREAMING_START;
	client->stream.app = "live";
	client->filePath = "receive.h264";
	ZeroMemory( &client->stat, sizeof StatisticInfo );
	while ( 0 != client->conn.connect_to( SERVER_IP, SERVER_PORT ) )
	{
		Sleep( 10 );
		continue;
	}
#ifdef _DEBUG
	RTMP_Log( RTMP_LOGDEBUG, "connect to %s:%d success.",
			  SERVER_IP, SERVER_PORT );
#endif // _DEBUG
	std::thread reciver( thread_func_for_receiver, client );
	std::thread writer( thread_func_for_writer, client );
	std::thread aliver( thread_func_for_aliver, client );
	std::thread controller( thread_func_for_controller, client );

	reciver.join( );
	writer.join( );
	aliver.join( );
	controller.join( );
#ifdef _DEBUG
	RTMP_LogThreadStop( );
#endif // _DEBUG
	Sleep( 10 );

	if ( client )
		free( client );
#ifdef _DEBUG
	if ( dumpfile )
		fclose( dumpfile );
#endif // _DEBUG
	cleanup_sockets( );
#ifdef _DEBUG
	_CrtDumpMemoryLeaks( );
#endif // _DEBUG
	return 0;
}

// Run program: Ctrl + F5 or Debug > Start Without Debugging menu
// Debug program: F5 or Debug > Start Debugging menu

// Tips for Getting Started: 
//   1. Use the Solution Explorer window to add/manage files
//   2. Use the Team Explorer window to connect to source control
//   3. Use the Output window to see build output and other messages
//   4. Use the Error List window to view errors
//   5. Go to Project > Add New Item to create new code files, or Project > Add Existing Item to add existing code files to the project
//   6. In the future, to open this project again, go to File > Open > Project and select the .sln file
