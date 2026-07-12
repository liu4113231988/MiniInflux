namespace MiniInflux.Net10.Storage;
public static class Varint
{
    public static void WriteUInt64(Stream s, ulong v){while(v>=0x80){s.WriteByte((byte)(v|0x80));v>>=7;}s.WriteByte((byte)v);}    
    public static ulong ReadUInt64(Stream s){int sh=0; ulong r=0; while(true){int b=s.ReadByte(); if(b<0) throw new EndOfStreamException(); r|=((ulong)(b&0x7F))<<sh; if((b&0x80)==0)return r; sh+=7;}}
    public static ulong ZigZag(long v)=>(ulong)((v<<1)^(v>>63));
    public static long UnZigZag(ulong v)=>(long)((v>>1)^((ulong)-(long)(v&1)));
}
