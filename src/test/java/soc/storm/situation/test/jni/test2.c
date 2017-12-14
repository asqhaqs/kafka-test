#include "string.h"
#include "stdio.h"
#include "aes_encry.h"

int main(int argc, char** argv)
{
	char *test_data_buff = "type:3accesstime:2014-11-27 09:35:59.045753sip:10.18.67.28sport:51001dip:192.168.0.223dport:53type:requesthost:quote";
	char encry_buff[512];
	char decrypt_buff[512]={0,};
	int len=strlen(test_data_buff);
	init_aes("decrypt.conf");
	printf("\nraw : %s\n",test_data_buff);
	len = encrypt(test_data_buff, len, encry_buff);
	printf("encrypt len : %d\n", len);
	int len2 = decrypt(encry_buff, len, decrypt_buff);
	printf("src_len:%d \n", len2);
	printf("out : %s\n", decrypt_buff);

	return 0;
}
