/*
 * stream_nfs.c
 *
 *  Created on: 2016-4-7
 *      Author: shi
 */
#define _GNU_SOURCE
#include "config.h"
#ifdef WIN32
#include <win32_compat.h>
#include <winsock2.h>
#pragma comment(lib, "ws2_32.lib")
WSADATA wsaData;
#define PRId64 "ll"
#else
#include <inttypes.h>
#include <string.h>
#include <sys/stat.h>
#ifndef AROS
#include <sys/statvfs.h>
#endif
#endif
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>
#include "mp_msg.h"
#include "network.h"
#include "stream.h"
#include "help_mp.h"
#include "m_option.h"
#include "m_struct.h"
//typedef void *caddr_t;
#include "nfsc/libnfs.h"
#include "nfsc/libnfs-raw.h"
#include "nfsc/libnfs-raw-nfs.h"
#include "nfsc/libnfs-raw-mount.h"

static struct stream_priv_s {
	char *server;
	char *export;
	uint32_t mount_port;
	struct nfsfh *nfssh;
	struct nfs_context *nfs;
	struct nfs_stat_64 stat;
	struct nfs_url *url;
	int is_finished;
    char * filename;
} stream_priv_dflts = {
};


static const m_option_t stream_opts_fields[] = {
  { NULL, NULL, 0, 0, 0, 0,  NULL }
};

static const struct m_struct_st stream_opts = {
  "nfs",
  sizeof(struct stream_priv_s),
  &stream_priv_dflts,
  stream_opts_fields
};

static int fill_buffer(stream_t *s, char* buffer, int max_len){
  struct stream_priv_s *client = s->priv;
  int r = nfs_read(client->nfs,client->nfssh,max_len,buffer);
//  fprintf(stderr,"max_len=%d,read_chunk=%d\n",max_len,s->read_chunk);
  return (r <= 0) ? -1 : r;
}

static int control_f(stream_t *s, int cmd, void *arg) {
	struct stream_priv_s *client = s->priv;
  switch(cmd) {
    case STREAM_CTRL_GET_SIZE: {
    	uint64_t offset;
      int ret = nfs_lseek(client->nfs,client->nfssh,0,SEEK_CUR,&offset);
      //fprintf(stderr,"control_ret=%d\n",ret);
      if(ret<0)
      {
    	  return -1;
      }
        *(uint64_t *)arg = offset;
        //fprintf(stderr,"offset=%llu\n",offset);
        return 1;
    }
  }
  return STREAM_UNSUPPORTED;
}

static int seek_f(stream_t *s, int64_t newpos) {
	struct stream_priv_s *client = s->priv;
	int ret;
	nfs_fstat64(client->nfs,client->nfssh,&client->stat);
	s->end_pos = client->stat.nfs_size;
//	fprintf(stderr,"pos=%d,new_pos=%d,seek_end_pos=%d\n",s->pos,newpos,s->end_pos);
	uint64_t offset;
	ret = nfs_lseek(client->nfs,client->nfssh,newpos,SEEK_SET,&offset);
	s->pos= (int64_t)offset;
//	fprintf(stderr,"s->pos=%llu\n",s->pos);
	if(ret < 0 )
	{
		return 0;
	}
	return 1;
}

/*static int write_buffer_f(stream_t *s, char* buffer, int len) {
	struct stream_priv_s *client = s->priv;
	uint64_t writesize;
	writesize = nfs_get_writemax(client->nfs);
	nfs_write(client->nfs,client.nfssh,len)
}*/

static void close_f(stream_t *s){
  struct stream_priv_s *client = s->priv;
  nfs_close(client->nfs,client->nfssh);
  m_struct_free(&stream_opts, client);
}

static int open_f (stream_t *stream, int mode, void *opts, int* file_format) {
  struct stream_priv_s *client =  opts; //nfs://10.1.2.2/opt/video/ac3_test.avi;
  client = malloc(sizeof(struct stream_priv_s));
  #ifdef WIN32
	if (WSAStartup(MAKEWORD(2,2), &wsaData) != 0) {
		printf("Failed to start Winsock2\n");
		exit(10);
	}
#endif
//  fprintf(stderr,"stream_nfs\n");
  client->nfs = nfs_init_context();
  //rpc_set_tcp_syncnt(nfs_get_rpc_context(client->nfs),2);
  if(client->nfs == NULL){
	  printf("failed to init context\n");
	  }
  client->url = nfs_parse_url_full(client->nfs,stream->url);
  if(client->url == NULL){
	  fprintf(stderr,"%s\n",nfs_get_error(client->nfs));
  }
  client->export = client->url->path;
  client->server = client->url->server;
  client->filename = client->url->file;
  client->filename = client->filename+1;   //ac3_test.avi
  if(nfs_mount(client->nfs,client->server,client->export) != 0){
	  fprintf(stderr,"Failed to mount nfs share:%s\n",nfs_get_error(client->nfs));
  }
//  fprintf(stderr,"server=%s,export=%s,filename=%s\n",client->server,client->export,client->filename);
  stream->read_chunk = nfs_get_readmax(client->nfs);
//  fprintf(stderr,"read_chunk=%d\n",stream->read_chunk);
if(nfs_open(client->nfs,client->filename,O_RDONLY,&client->nfssh) < 0)
{
	fprintf(stderr,"open file error\n");
	return -1;
}

//	nfs_fstat64(client->nfs,client->nfssh,&client->stat);
  stream->type = STREAMTYPE_NFS;
  stream->priv = client;
  stream->fd = -1;
  stream->flags = STREAM_READ|MP_STREAM_SEEK;
  stream->fill_buffer = fill_buffer;
  stream->seek = seek_f;
  stream->start_pos = 0;
//  stream->write_buffer = write_buffer_f;
  stream->close = close_f;
  stream->control = control_f;
//  stream->end_pos = client->stat.nfs_size;
//  fprintf(stderr,"end_pos=%llu\n",stream->end_pos);
//  m_struct_free(&stream_opts, opts);
  return STREAM_OK;
}

const stream_info_t stream_info_nfs = {
  "Server Message Block",
  "nfs",
  "shi.xinhao",
  "based on the code from libnfs",
  open_f,
  {"nfs", NULL},
  &stream_opts,
  0 //Url is an option string
};



