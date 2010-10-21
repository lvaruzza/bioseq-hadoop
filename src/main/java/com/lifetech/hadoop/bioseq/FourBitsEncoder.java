package com.lifetech.hadoop.bioseq;

/*
 * codes:
 * 'A' => 0000,   0x00
 * 'C' => 0001,   0x01
 * 'G' => 0010,   0x
 * 'T' => 0011,
 * 'N' => 0100,
 * '0' => 1000,
 * '1' => 1001,
 * '2' => 1010,
 * '3' => 1011,
 * '.' => 1100
 * 
 *  padding 1111
 *  00 => A
 *  01 => C
 *  02 => G
 *  03 => T
 *  04 => N
 *  08 => 0
 *  09 => 1
 *  0A => 2
 *  0B => 3
 *  0C => .
 *  
 *  0T 21
 */

public class FourBitsEncoder extends BioSeqEncoder {

	private static byte[] encodingVector={
			0x04 , // 0x0
			0x04 , // 0x1
			0x04 , // 0x2
			0x04 , // 0x3
			0x04 , // 0x4
			0x04 , // 0x5
			0x04 , // 0x6
			0x04 , // 0x7
			0x04 , // 0x8
			0x04 , // 0x9
			0x04 , // 0xa
			0x04 , // 0xb
			0x04 , // 0xc
			0x04 , // 0xd
			0x04 , // 0xe
			0x04 , // 0xf
			0x04 , // 0x10
			0x04 , // 0x11
			0x04 , // 0x12
			0x04 , // 0x13
			0x04 , // 0x14
			0x04 , // 0x15
			0x04 , // 0x16
			0x04 , // 0x17
			0x04 , // 0x18
			0x04 , // 0x19
			0x04 , // 0x1a
			0x04 , // 0x1b
			0x04 , // 0x1c
			0x04 , // 0x1d
			0x04 , // 0x1e
			0x04 , // 0x1f
			0x04 , //  
			0x04 , // !
			0x04 , // "
			0x04 , // #
			0x04 , // $
			0x04 , // %
			0x04 , // &
			0x04 , // '
			0x04 , // (
			0x04 , // )
			0x04 , // *
			0x04 , // +
			0x04 , // ,
			0x04 , // -
			0x0c , // .
			0x04 , // /
			0x08 , // 0
			0x09 , // 1
			0x0a , // 2
			0x0b , // 3
			0x04 , // 4
			0x04 , // 5
			0x04 , // 6
			0x04 , // 7
			0x04 , // 8
			0x04 , // 9
			0x04 , // :
			0x04 , // ;
			0x04 , // <
			0x04 , // =
			0x04 , // >
			0x04 , // ?
			0x04 , // @
			0x00 , // A
			0x04 , // B
			0x01 , // C
			0x04 , // D
			0x04 , // E
			0x04 , // F
			0x02 , // G
			0x04 , // H
			0x04 , // I
			0x04 , // J
			0x04 , // K
			0x04 , // L
			0x04 , // M
			0x04 , // N
			0x04 , // O
			0x04 , // P
			0x04 , // Q
			0x04 , // R
			0x04 , // S
			0x03 , // T
			0x04 , // U
			0x04 , // V
			0x04 , // W
			0x04 , // X
			0x04 , // Y
			0x04 , // Z
			0x04 , // [
			0x04 , // \
			0x04 , // ]
			0x04 , // ^
			0x04 , // _
			0x04 , // `
			0x00 , // a
			0x04 , // b
			0x01 , // c
			0x04 , // d
			0x04 , // e
			0x04 , // f
			0x02 , // g
			0x04 , // h
			0x04 , // i
			0x04 , // j
			0x04 , // k
			0x04 , // l
			0x04 , // m
			0x04 , // n
			0x04 , // o
			0x04 , // p
			0x04 , // q
			0x04 , // r
			0x04 , // s
			0x03 , // t
			0x04 , // u
			0x04 , // v
			0x04 , // w
			0x04 , // x
			0x04 , // y
			0x04 , // z
			0x04 , // {
			0x04 , // |
			0x04 , // }
			0x04 , // ~
			0x04 , // 0x7f
			0x04 , // 0x80
			0x04 , // 0x81
			0x04 , // 0x82
			0x04 , // 0x83
			0x04 , // 0x84
			0x04 , // 0x85
			0x04 , // 0x86
			0x04 , // 0x87
			0x04 , // 0x88
			0x04 , // 0x89
			0x04 , // 0x8a
			0x04 , // 0x8b
			0x04 , // 0x8c
			0x04 , // 0x8d
			0x04 , // 0x8e
			0x04 , // 0x8f
			0x04 , // 0x90
			0x04 , // 0x91
			0x04 , // 0x92
			0x04 , // 0x93
			0x04 , // 0x94
			0x04 , // 0x95
			0x04 , // 0x96
			0x04 , // 0x97
			0x04 , // 0x98
			0x04 , // 0x99
			0x04 , // 0x9a
			0x04 , // 0x9b
			0x04 , // 0x9c
			0x04 , // 0x9d
			0x04 , // 0x9e
			0x04 , // 0x9f
			0x04 , // 0xa0
			0x04 , // 0xa1
			0x04 , // 0xa2
			0x04 , // 0xa3
			0x04 , // 0xa4
			0x04 , // 0xa5
			0x04 , // 0xa6
			0x04 , // 0xa7
			0x04 , // 0xa8
			0x04 , // 0xa9
			0x04 , // 0xaa
			0x04 , // 0xab
			0x04 , // 0xac
			0x04 , // 0xad
			0x04 , // 0xae
			0x04 , // 0xaf
			0x04 , // 0xb0
			0x04 , // 0xb1
			0x04 , // 0xb2
			0x04 , // 0xb3
			0x04 , // 0xb4
			0x04 , // 0xb5
			0x04 , // 0xb6
			0x04 , // 0xb7
			0x04 , // 0xb8
			0x04 , // 0xb9
			0x04 , // 0xba
			0x04 , // 0xbb
			0x04 , // 0xbc
			0x04 , // 0xbd
			0x04 , // 0xbe
			0x04 , // 0xbf
			0x04 , // 0xc0
			0x04 , // 0xc1
			0x04 , // 0xc2
			0x04 , // 0xc3
			0x04 , // 0xc4
			0x04 , // 0xc5
			0x04 , // 0xc6
			0x04 , // 0xc7
			0x04 , // 0xc8
			0x04 , // 0xc9
			0x04 , // 0xca
			0x04 , // 0xcb
			0x04 , // 0xcc
			0x04 , // 0xcd
			0x04 , // 0xce
			0x04 , // 0xcf
			0x04 , // 0xd0
			0x04 , // 0xd1
			0x04 , // 0xd2
			0x04 , // 0xd3
			0x04 , // 0xd4
			0x04 , // 0xd5
			0x04 , // 0xd6
			0x04 , // 0xd7
			0x04 , // 0xd8
			0x04 , // 0xd9
			0x04 , // 0xda
			0x04 , // 0xdb
			0x04 , // 0xdc
			0x04 , // 0xdd
			0x04 , // 0xde
			0x04 , // 0xdf
			0x04 , // 0xe0
			0x04 , // 0xe1
			0x04 , // 0xe2
			0x04 , // 0xe3
			0x04 , // 0xe4
			0x04 , // 0xe5
			0x04 , // 0xe6
			0x04 , // 0xe7
			0x04 , // 0xe8
			0x04 , // 0xe9
			0x04 , // 0xea
			0x04 , // 0xeb
			0x04 , // 0xec
			0x04 , // 0xed
			0x04 , // 0xee
			0x04 , // 0xef
			0x04 , // 0xf0
			0x04 , // 0xf1
			0x04 , // 0xf2
			0x04 , // 0xf3
			0x04 , // 0xf4
			0x04 , // 0xf5
			0x04 , // 0xf6
			0x04 , // 0xf7
			0x04 , // 0xf8
			0x04 , // 0xf9
			0x04 , // 0xfa
			0x04 , // 0xfb
			0x04 , // 0xfc
			0x04 , // 0xfd
			0x04 , // 0xfe
			0x04  // 0xff
		}; 

	@Override
	public byte[] decode(byte[] data, int size) {
		return decode(data,0,size);
	}
	
	@Override
	public byte[] decode(byte[] data, int start,int size) {
		int newsize = (size << 1);
		// test for padding
		if ((data[size-1] & 0x0f) == 0x0f) {
			newsize--;
		}
		byte[] r = new byte[newsize];
		
		for(int i=start,j=0;i<size;i++,j+=2) {
			byte fst = (byte) ((data[i] >> 4) & 0x07);
			byte snd = (byte) (data[i] & 0x07);
			System.out.printf("D: %d fst = %x %d snd = %x\n",i<<1,fst,(j)+1,snd);
			
			r[j] = (byte) ((data[i] & 0x80) == 0x80 ? colors[fst] : bases[fst]);
			if (snd == 7) break;
			r[j + 1] = (byte) ((data[i] & 0x08) == 0x08 ? colors[snd] : bases[snd]);
		}
		return r;
	}

	public void printBytes(byte[] x,int size) {
		for(int i=0;i<size;i++) {
			System.out.printf("%x ",x[i]);
		}
		System.out.print("\n");
	}

	@Override
	public byte[] encode(byte[] data, int size) {
		return encode(data,0,size);
	}
	
	@Override
	public byte[] encode(byte[] data, int start,int size) {
		int newsize = ((size+1) >> 1);
		byte[] r = new byte[newsize];
		
		for(int i=start,j=0;i<size;i+=2,j++) {
			r[j] = (byte) (encodingVector[data[i]] <<4);
			
			if (i-start+1 < size) {
				r[j] += (byte) (encodingVector[data[i+1]]);
				System.out.printf("E: %d  %c %c r=%x\n",i,data[i],data[i+1],r[j]);
			} else {
				r[j] += 0x0f;
				System.out.printf("E: %d %c pad r=%x\n",i,data[i],r[j]);
			}
		}
		return r;
	}
}
