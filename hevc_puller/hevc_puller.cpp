/*
 * Copyright (C) 2020 BrianYi, All rights reserved
 */

#include <winsock2.h>
#include <iostream>
#include <vector>
#include <thread>
#include <string>
#include <queue>
#include <mutex>
#include "TCP.h"
#include "Log.h"
#define TIME_CACULATE
#include "Packet.h"

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

//#define BUFSIZE 10 * 1024
#define SERVER_IP "192.168.1.105"
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

struct STREAMING_PULLER
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
stopStreaming( STREAMING_PULLER * puller )
{
	if ( puller->state != STREAMING_STOPPED )
	{
		if ( puller->state == STREAMING_IN_PROGRESS )
		{
			puller->state = STREAMING_STOPPING;

			// wait for streaming threads to exit
			while ( puller->state != STREAMING_STOPPED )
				Sleep( 10 );
		}
		puller->state = STREAMING_STOPPED;
	}
}

inline fd_set get_fd_set( STREAMING_PULLER* puller )
{
	fd_set fdSet;
	FD_ZERO( &fdSet );
	FD_SET( puller->conn.m_socketID, &fdSet );
	return fdSet;
}


// std::mutex mux;
// std::condition_variable cond;
int thread_func_for_receiver( void *arg )
{
	RTMP_Log( RTMP_LOGDEBUG, "receiver thread is start..." );
	STREAMING_PULLER* puller = ( STREAMING_PULLER* ) arg;
	StreamInfo& stream = puller->stream;
	size_t timebase = stream.timebase;
	fd_set fdSet = get_fd_set( puller );
	timeval tm { 0,100 }; // 设置超时时间
	bool isAck = false;
	while ( !isAck )
	{
		while ( select( 0, nullptr, &fdSet, nullptr, &tm ) <= 0 )
		{
			fdSet = get_fd_set( puller );
			Sleep( 10 );
		}
		send_play_packet( puller->conn,
						  get_current_milli( ),
						  puller->stream.app.c_str( ) );
		//Sleep( 10 );	// wait for packet comming

		// recv ack
		PACKET pkt;
		if ( recv_packet( puller->conn, pkt ) <= 0 )
			continue;
		switch ( pkt.header.type )
		{
		case Ack:
			printf( "Begin to receive stream.\n" );
			puller->stream.timebase = pkt.header.reserved;
			isAck = true;
			//cond.notify_one( );
			break;
		case Err:
			printf( "Server has no stream for name %s\n", pkt.header.app );
			break;
		default:
			printf( "unknown packet.\n" );
		}
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
	int32_t totalSize = 0;
	while ( puller->state == STREAMING_START )
	{
#ifdef _DEBUG
		TIME_BEG( 4 ); // 117ms 274ms  1297ms 117ms  274ms 1297ms 117ms 274ms 1297ms 117ms 274ms
#endif // _DEBUG
		// receive packet
		PACKET pkt;
		timeval tm { 0,100 };
		fd_set fdSet = get_fd_set( puller );
		while ( select( 0, &fdSet, nullptr, nullptr, &tm ) <= 0 &&
				puller->state == STREAMING_START )
		{
			fdSet = get_fd_set( puller );
			Sleep( 10 );
		}

#ifdef _DEBUG
		TIME_END( 4 );
#endif // _DEBUG

#ifdef _DEBUG
		TIME_BEG( 5 );
#endif // _DEBUG
		if ( recv_packet( puller->conn, pkt, NonBlocking ) <= 0 ) // no packet, continue loop next
		{
			RTMP_LogAndPrintf( RTMP_LOGERROR, "recv_packet packet error %s:%d", __FUNCTION__, __LINE__ );
			break;//break;
		}
#ifdef _DEBUG
		if ( INVALID_PACK( pkt.header ) )
		{
			RTMP_LogAndPrintf( RTMP_LOGERROR, "Invalid packet %s:%d", __FUNCTION__, __LINE__ );
			stopStreaming( puller );
			return -1;
		}
		caculate_statistc( puller->stat, pkt, StatRecv );
#endif // _DEBUG

		if ( maxRecvBuf < pkt.header.size )
		{
			maxRecvBuf = ( pkt.header.size + MAX_PACKET_SIZE - 1 ) / MAX_PACKET_SIZE * MAX_PACKET_SIZE;
			puller->conn.set_socket_rcvbuf_size( maxRecvBuf );
		}
#ifdef _DEBUG
		TIME_END( 5 );
#endif // _DEBUG
		//MP = pkt.header.MP;
		switch ( pkt.header.type )
		{
		case Pull:
		{
			if ( stream.app != pkt.header.app )
			{
				send_err_packet( puller->conn,
								 get_current_milli( ),
								 pkt.header.app );
			}

			if ( priq.empty( ) )
			{
				frame.timestamp = pkt.header.timestamp;
				frame.size = pkt.header.size;
				frame.data = ( char * ) malloc( frame.size );
			}

			if ( frame.timestamp == pkt.header.timestamp )
			{
				priq.push( pkt );
				totalSize += BODY_SIZE_H( pkt.header );
#ifdef _DEBUG
				RTMP_Log( RTMP_LOGDEBUG, "priq.size()==%d totalSize=%d frame.size=%d", priq.size( ), totalSize, frame.size );
#endif // _DEBUG
			}
			else
			{
#ifdef _DEBUG
				RTMP_LogAndPrintf( RTMP_LOGERROR, "recv packet is incomplete %s:%d", __FUNCTION__, __LINE__ );
				stopStreaming(puller); // if enter here, wrong
				return -1;
#endif // _DEBUG
			}


			// full frame
			if ( totalSize == frame.size )
			{
#ifdef _DEBUG
				TIME_BEG( 6 );
#endif // _DEBUG
				PACKET tmpPack;
				int idx = 0;
				while ( !priq.empty( ) )
				{
					tmpPack = priq.top( );
					priq.pop( );
					int bodySize = BODY_SIZE_H( tmpPack.header );
					memcpy( &frame.data[ idx ], tmpPack.body, bodySize );
					idx += bodySize;
				}
				std::unique_lock<std::mutex> lock( puller->mux );
				stream.frameData.push( frame );
#ifdef _DEBUG
				RTMP_Log( RTMP_LOGDEBUG, "priq.size()==%d, push one frame", priq.size( ) );
#endif // _DEBUG
				lock.unlock( );
				totalSize = 0;
#ifdef _DEBUG
				TIME_END( 6 );
#endif // _DEBUG
			}
			break;
		}
		case Fin:
		{
			if ( stream.app != pkt.header.app )
			{
				send_err_packet( puller->conn,
								 get_current_milli( ),
								 pkt.header.app );
			}
			stopStreaming( puller );
			break;
		}
		default:
			RTMP_Log( RTMP_LOGDEBUG, "unknown packet." );
			break;
		}
	}
	RTMP_Log( RTMP_LOGDEBUG, "receiver thread is quit." );
	return true;
}

int thread_func_for_writer( void *arg )
{
	RTMP_LogPrintf( "writer thread is start...\n" );
	STREAMING_PULLER *puller = ( STREAMING_PULLER * ) arg;

	FILE *fp = fopen( puller->filePath.c_str( ), "wb" );
	if ( !fp )
	{
		RTMP_LogPrintf( "Open File Error.\n" );
		stopStreaming( puller );
		return -1;
	}
// 
// 	std::unique_lock<std::mutex> locker( mux );
// 	cond.wait( locker );
// 	locker.unlock( );

	FrameData& frameData = puller->stream.frameData;
	int64_t currentTime = 0, waitTime = 0;
	while ( puller->state == STREAMING_START )
	{
#ifdef _DEBUG
		TIME_BEG( 1 );
#endif // _DEBUG
		if ( frameData.empty( ) )
		{
			Sleep( 5 );
			continue;
		}
#ifdef _DEBUG
		TIME_END( 1 );
#endif // _DEBUG

#ifdef _DEBUG
		TIME_BEG( 2 );	//134ms 134ms 134ms 134ms
#endif // _DEBUG
		std::unique_lock<std::mutex> lock( puller->mux );
		Frame frame = frameData.front( );
		frameData.pop( );
		lock.unlock( );

		currentTime = get_current_milli( );
		waitTime = frame.timestamp - currentTime;
		if ( waitTime >= 0 )
			Sleep( waitTime );
		fwrite( frame.data, 1, frame.size, fp );
#ifdef _DEBUG
		TIME_END( 2 );
#endif // _DEBUG
#ifdef _DEBUG
		TIME_BEG( 3 );
		int64_t writeTimestamp = get_current_milli( );
		RTMP_Log( RTMP_LOGDEBUG, "write frame %dB, frame timestamp=%lld, write timestamp=%lld, W-F=%lld",
				  frame.size,
				  frame.timestamp,
				  writeTimestamp,
				  writeTimestamp - frame.timestamp );
		TIME_END( 3 );
#endif // _DEBUG
		free( frame.data );
	}

	fclose( fp );
	RTMP_LogPrintf( "writer thread is quit.\n" );
	return true;
}

void show_statistics( STREAMING_PULLER* puller )
{
	printf( "%-15s%-6s%-8s%-20s %-8s\t\t%-13s\t%-10s\t%-15s\t %-8s\t%-13s\t%-10s\t%-15s\n",
			"ip", "port", "type", "app",
			"rec-byte", "rec-byte-rate", "rec-packet", "rec-packet-rate",
			"snd-byte", "snd-byte-rate", "snd-packet", "snd-packet-rate" );


	printf( "%-15s%-6d%-8s%-20s %-6.2fMB\t\t%-9.2fKB/s\t%-10lld\t%-13lld/s\t %-6.2fMB\t%-9.2fKB/s\t%-10lld\t%-13lld/s\n",
			puller->conn.getIP( ).c_str( ),
			puller->conn.getPort( ),
			"Puller",
			puller->stream.app.c_str(),

			MB( puller->stat.recvBytes ),
			KB( puller->stat.recvByteRate ),
			puller->stat.recvPackets,
			puller->stat.recvPacketRate,

			MB( puller->stat.sendBytes ),
			KB( puller->stat.sendByteRate ),
			puller->stat.sendPackets,
			puller->stat.sendPacketRate );

}

int thread_func_for_controller( void *arg )
{
	RTMP_LogPrintf( "controller thread is start...\n" );
	STREAMING_PULLER *puller = ( STREAMING_PULLER * ) arg;
	std::string choice;
	while ( puller->state == STREAMING_START )
	{
		system( "cls" );
		show_statistics( puller );
		Sleep( 1000 );
// 		std::cin >> choice;
// 		if ( choice == "quit" || choice == "q" || choice == "exit" )
// 		{
// 			RTMP_LogAndPrintf( RTMP_LOGDEBUG, "Exiting" );
// 			stopStreaming( puller );
// 		}
// 		else if ( choice == "status" || choice == "s" )
// 		{
// 			show_statistics( puller );
// 		}
	}
	RTMP_LogPrintf( "controller thread is quit.\n" );
	return 0;
}

// int thread_func_for_aliver( void *arg )
// {
// 	RTMP_Log( RTMP_LOGDEBUG, "cleaner thread is start..." );
// 	STREAMING_PULLER* puller = ( STREAMING_PULLER* ) arg;
// 	StreamInfo& streamInfo = puller->stream;
// 	while ( puller->state == STREAMING_START )
// 	{
// 		// send heart packet
// 		send_alive_packet( puller->conn,
// 						   get_current_milli( ),
// 						   streamInfo.app.c_str( ) );
// 		Sleep( 1000 );
// 	}
// 	RTMP_Log( RTMP_LOGDEBUG, "cleaner thread is quit." );
// 	return true;
// }

int main( int argc, char* argv[ ] )
{
	if ( argc < 3 )
	{
		printf( "please pass in live name and file path parameter.\n" );
		printf( "usage: puller \"live-name\" \"/path/to/save/file\" \n" );
		return 0;
	}
#ifdef _DEBUG
	FILE* dumpfile = nullptr;
	if ( argv[ 3 ] )
		dumpfile = fopen( argv[ 3 ], "a+" );
	else
		dumpfile = fopen( "hevc_puller.dump", "a+" );
	RTMP_LogSetOutput( dumpfile );
	RTMP_LogSetLevel( RTMP_LOGALL );
	RTMP_LogThreadStart( );

	SYSTEMTIME tm;
	GetSystemTime( &tm );
	RTMP_Log( RTMP_LOGDEBUG, "==============================" );
	RTMP_Log( RTMP_LOGDEBUG, "log file:\thevc_puller.dump" );
	RTMP_Log( RTMP_LOGDEBUG, "log timestamp:\t%lld", get_current_milli( ) );
	RTMP_Log( RTMP_LOGDEBUG, "log date:\t%d-%d-%d %d:%d:%d.%d",
			  tm.wYear,
			  tm.wMonth,
			  tm.wDay,
			  tm.wHour + 8, tm.wMinute, tm.wSecond, tm.wMilliseconds );
	RTMP_Log( RTMP_LOGDEBUG, "==============================" );
#endif
	init_sockets( );

	STREAMING_PULLER *puller = new STREAMING_PULLER;
	puller->state = STREAMING_START;
	puller->stream.app = argv[ 1 ];
	puller->filePath = argv[ 2 ];
	ZeroMemory( &puller->stat, sizeof StatisticInfo );
	while ( 0 != puller->conn.connect_to( SERVER_IP, SERVER_PORT ) )
	{
		printf( "Connect to server %s:%d failed.\n", SERVER_IP, SERVER_PORT );
		Sleep( 1000 );
		continue;
	}
	printf( "Successful connected.\n");
	std::thread reciver( thread_func_for_receiver, puller );
	std::thread writer( thread_func_for_writer, puller );
	//std::thread aliver( thread_func_for_aliver, puller );
	std::thread controller( thread_func_for_controller, puller );

	reciver.join( );
	writer.join( );
	//aliver.join( );
	controller.join( );
#ifdef _DEBUG
	RTMP_LogThreadStop( );
#endif // _DEBUG
	Sleep( 10 );

	if ( puller )
		free( puller );
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
