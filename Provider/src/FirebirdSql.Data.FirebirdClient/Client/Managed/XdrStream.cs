/*
 *	Firebird ADO.NET Data provider for .NET and Mono
 *
 *	   The contents of this file are subject to the Initial
 *	   Developer's Public License Version 1.0 (the "License");
 *	   you may not use this file except in compliance with the
 *	   License. You may obtain a copy of the License at
 *	   http://www.firebirdsql.org/index.php?op=doc&id=idpl
 *
 *	   Software distributed under the License is distributed on
 *	   an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either
 *	   express or implied. See the License for the specific
 *	   language governing rights and limitations under the License.
 *
 *	Copyright (c) 2002, 2007 Carlos Guzman Alvarez
 *	Copyright (c) 2014 - 2017 Jiri Cincura (jiri@cincura.net)
 *	All Rights Reserved.
 */

using System;
using System.Collections;
using System.IO;
using System.Net;
using System.Text;
using System.Globalization;
using System.Linq;
using FirebirdSql.Data.Common;
using System.Collections.Generic;
using Ionic.Zlib;

namespace FirebirdSql.Data.Client.Managed
{
	internal class XdrStream : Stream
	{
		#region Constants

		private const int PreferredBufferSize = 32 * 1024;

		#endregion

		#region Static Fields

		private static byte[] pad;

		#endregion

		#region Static Properties

		private static byte[] Pad
		{
			get { return pad ?? (pad = new byte[] { 0, 0, 0, 0 }); }
		}

		#endregion

		#region Fields

		private Stream _innerStream;
		private Charset _charset;
		private readonly bool _compression;
		private bool _ownsStream;

		private XdrBinaryWriter _outputWriter;
		// do not dispose reader to prevent unconditional dispose of _innerStream
		private XdrBinaryReader _inputReader;

		private Ionic.Zlib.ZlibCodec _deflate;
		private byte[] _compressionBuffer;

		private int _operation;

		#endregion

		#region Stream Properties

		public override bool CanWrite
		{
			get { return _innerStream.CanWrite; }
		}

		public override bool CanRead
		{
			get { return _innerStream.CanRead; }
		}

		public override bool CanSeek
		{
			get { return _innerStream.CanSeek; }
		}

		public override long Position
		{
			get { return _inputReader.BaseStream.Position; }
			set { throw new NotSupportedException(); }
		}

		public override long Length
		{
			get { return _innerStream.Length; }
		}

		#endregion

		#region Constructors

		public XdrStream()
			: this(Charset.DefaultCharset)
		{ }

		public XdrStream(Charset charset)
			: this(new MemoryStream(), charset, false, true)
		{ }

		public XdrStream(byte[] buffer, Charset charset)
			: this(new MemoryStream(buffer), charset, false, true)
		{ }

		public XdrStream(Stream innerStream, Charset charset, bool compression, bool ownsStream)
			: base()
		{
			_innerStream = innerStream;
			_charset = charset;
			_compression = compression;
			_ownsStream = ownsStream;

			var streamWrapper = _innerStream;
			if (compression)
				streamWrapper = new DecompressionStream(_innerStream, PreferredBufferSize, 1024 * 1024);
			else if (!(_innerStream is MemoryStream))
				streamWrapper = new BufferedStream(_innerStream, PreferredBufferSize);

			_inputReader = new XdrBinaryReader(streamWrapper, _charset);
			_outputWriter = new XdrBinaryWriter(new MemoryStream(), _charset);
			ResetOperation();
		}

		#endregion

		#region Stream methods

		protected override void Dispose(bool disposing)
		{
			if (disposing)
			{
				if (_ownsStream)
				{
					_innerStream?.Dispose();
				}
				_innerStream = null;
				_charset = null;
			}
		}

		public override void Flush()
		{
			CheckDisposed();

			var ms = (MemoryStream) _outputWriter.BaseStream;

			var buffer = ms.ToArraySegment();
			try
			{
				if (_compression)
				{
					if (_deflate == null) _deflate = new Ionic.Zlib.ZlibCodec(Ionic.Zlib.CompressionMode.Compress);
					if (_compressionBuffer == null) _compressionBuffer = new byte[1024*1024];

					_deflate.OutputBuffer = _compressionBuffer;
					_deflate.AvailableBytesOut = _compressionBuffer.Length;
					_deflate.NextOut = 0;
					_deflate.InputBuffer = buffer.Array;
					_deflate.AvailableBytesIn = buffer.Count;
					_deflate.NextIn = 0;
					var rc = _deflate.Deflate(Ionic.Zlib.FlushType.Sync);
					if (rc != Ionic.Zlib.ZlibConstants.Z_OK)
						throw new IOException($"Error '{rc}' while compressing the data.");
					if (_deflate.AvailableBytesIn != 0)
						throw new IOException("Compression buffer too small.");
					buffer = new ArraySegment<byte>(_compressionBuffer, 0, _deflate.NextOut);
				}
				_innerStream.Write(buffer.Array, buffer.Offset, buffer.Count);
				_innerStream.Flush();
				_inputReader.BaseStream.Flush();
			}
			finally
			{
				_outputWriter.BaseStream.SetLength(0);
			}
		}

		public override void SetLength(long length)
		{
			CheckDisposed();

			throw new NotSupportedException();
		}

		public override long Seek(long offset, SeekOrigin loc)
		{
			CheckDisposed();

			throw new NotSupportedException();
		}

		public override int ReadByte()
		{
			CheckDisposed();
			EnsureReadable();

			throw new NotSupportedException();
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			CheckDisposed();
			EnsureReadable();
			if (count == 0) return 0;

			return _inputReader.Read(buffer, offset, count);
		}

		public override void WriteByte(byte value)
		{
			CheckDisposed();
			EnsureWritable();

			_outputWriter.Write(value);
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			CheckDisposed();
			EnsureWritable();
			_outputWriter.Write(buffer, offset, count);
		}

		#endregion

		#region Operation Identification Methods

		public int ReadOperation()
		{
			var op = ValidOperationAvailable ? _operation : ReadNextOperation();
			ResetOperation();
			return op;
		}

		public int ReadNextOperation()
		{
			do
			{
				/* loop	as long	as we are receiving	dummy packets, just
				 * throwing	them away--note	that if	we are a server	we won't
				 * be receiving	them, but it is	better to check	for	them at
				 * this	level rather than try to catch them	in all places where
				 * this	routine	is called
				 */
				_operation = ReadInt32();
			} while (_operation == IscCodes.op_dummy);

			return _operation;
		}

		public void SetOperation(int operation)
		{
			_operation = operation;
		}

		private void ResetOperation()
		{
			_operation = -1;
		}

		#endregion

		#region XDR Read Methods

		public byte[] ReadBytes(int count) => _inputReader.ReadBytes(count);
		public byte[] ReadOpaque(int length) => _inputReader.ReadOpaque(length);
		public byte[] ReadBuffer() => _inputReader.ReadBuffer();

		public string ReadString() => _inputReader.ReadString();
		public string ReadString(int length) => _inputReader.ReadString(length);
		public string ReadString(Charset charset) => _inputReader.ReadString(charset);
		public string ReadString(Charset charset, int length) => _inputReader.ReadString(charset, length);

		public short ReadInt16() => _inputReader.ReadInt16();
		public int ReadInt32() => _inputReader.ReadInt32();
		public long ReadInt64() => _inputReader.ReadInt64();

		public Guid ReadGuid(int length) => _inputReader.ReadGuid();

		public float ReadSingle() => _inputReader.ReadSingle();
		public double ReadDouble() => _inputReader.ReadDouble();

		public DateTime ReadDateTime() => _inputReader.ReadDateTime();
		public DateTime ReadDate() => _inputReader.ReadDate();
		public TimeSpan ReadTime() => _inputReader.ReadTime();

		public decimal ReadDecimal(int type, int scale) => _inputReader.ReadDecimal(type, scale);
		public bool ReadBoolean() => _inputReader.ReadBoolean();
		public IscException ReadStatusVector() => _inputReader.ReadStatusVector();

		public void Skip(int count) => _inputReader.Skip(count);

		#endregion

		#region XDR Write Methods

		public void WriteOpaque(byte[] buffer) => _outputWriter.WriteOpaque(buffer);
		public void WriteOpaque(byte[] buffer, int length) => _outputWriter.WriteOpaque(buffer, length);
		public void WriteBuffer(byte[] buffer) => _outputWriter.WriteBuffer(buffer);
		public void WriteBuffer(ArraySegment<byte> buffer) => _outputWriter.WriteBuffer(buffer);
		public void WriteBuffer(byte[] buffer, int length) => _outputWriter.WriteBuffer(buffer, length);

		public void WriteBlobBuffer(byte[] buffer)
		{
			var length = buffer.Length; // 2 for short for buffer length
			if (length > short.MaxValue)
				throw new IOException();
			Write(length + 2);
			Write(length + 2);  //bizarre but true! three copies of the length
			WriteByte((byte)((length >> 0) & 0xff));
			WriteByte((byte)((length >> 8) & 0xff));
			Write(buffer, 0, length);
			Write(Pad, 0, ((4 - length + 2) & 3));
		}

		public void WriteTyped(int type, byte[] buffer)
		{
			int length;
			if (buffer == null)
			{
				Write(1);
				WriteByte((byte)type);
				length = 1;
			}
			else
			{
				length = buffer.Length + 1;
				Write(length);
				WriteByte((byte)type);
				Write(buffer, 0, buffer.Length);
			}
			Write(Pad, 0, ((4 - length) & 3));
		}

		public void Write(ArraySegment<byte> buffer) => _outputWriter.Write(buffer);
		public void Write(string value) => _outputWriter.Write(value);
		public void Write(short value) => _outputWriter.Write(value);
		public void Write(int value) => _outputWriter.Write(value);
		public void Write(long value) => _outputWriter.Write(value);
		public void Write(float value) => _outputWriter.Write(value);
		public void Write(double value) => _outputWriter.Write(value);
		public void Write(decimal value, int type, int scale) => _outputWriter.Write(value, type, scale);
		public void Write(bool value) => _outputWriter.Write(value);
		public void Write(DateTime value) => _outputWriter.Write(value);
		public void WriteDate(DateTime value) => _outputWriter.WriteDate(value);
		public void WriteTime(TimeSpan value) => _outputWriter.WriteTime(value);

		#endregion

		#region Private Methods

		private void CheckDisposed()
		{
			if (_innerStream == null)
				throw new ObjectDisposedException($"The {nameof(XdrStream)} is closed.");
		}

		private void EnsureWritable()
		{
			if (!CanWrite)
				throw new InvalidOperationException("Write operations are not allowed by this stream.");
		}

		private void EnsureReadable()
		{
			if (!CanRead)
				throw new InvalidOperationException("Read operations are not allowed by this stream.");
		}

		#endregion

		#region Private Properties

		private bool ValidOperationAvailable
		{
			get { return _operation >= 0; }
		}

		#endregion
	}

	internal class XdrBinaryReader
	{
		private readonly Stream _stream;
		private readonly Charset _charset;
		private readonly byte[] _innerBuffer = new byte[128];
		private static readonly byte[] _pad = new byte[4];

		public XdrBinaryReader(Stream stream, Charset charset)
		{
			_stream = stream;
			_charset = charset;
		}

		public Stream BaseStream => _stream;

		private byte[] InternalReadBuffer(int count, bool opaque = false)
		{
			var result = count > _innerBuffer.Length ? new byte[count] : _innerBuffer;
			InternalReadBuffer(result, 0, count, opaque);
			return result;
		}


		private void InternalReadBuffer(byte[] buffer, int offset, int count, bool opaque = false)
		{
			var needToRead = count;
			while (needToRead > 0)
			{
				var readed = Read(buffer, offset, needToRead);
				if (readed == 0)
					throw new IOException();
				needToRead -= readed;
				offset += readed;
			}

			if (opaque)
			{
				var padLength = ((4 - count) & 3);
				if (padLength > 0)
				{
					Read(_pad, 0, padLength);
				}
			}
		}

		public int Read(byte[] buffer, int offset, int count) => _stream.Read(buffer, offset, count);

		public byte[] ReadBytes(int count)
		{
			var result = new byte[count];
			InternalReadBuffer(result, 0, count);
			return result;
		}

		public byte[] ReadBuffer()
		{
			var result = new byte[(ushort) ReadInt32()];
			InternalReadBuffer(result, 0, result.Length, opaque: true);
			return result;
		}

		public byte[] ReadOpaque(int count)
		{
			var result = new byte[count];
			InternalReadBuffer(result, 0, count, opaque: true);
			return result;
		}

		public int ReadInt32() => IPAddress.HostToNetworkOrder(BitConverter.ToInt32(InternalReadBuffer(4), 0));
		public short ReadInt16() => Convert.ToInt16(ReadInt32());
		public long ReadInt64() => IPAddress.HostToNetworkOrder(BitConverter.ToInt64(InternalReadBuffer(8), 0));
		public double ReadDouble() => BitConverter.Int64BitsToDouble(ReadInt64());
		public float ReadSingle() => BitConverter.ToSingle(BitConverter.GetBytes(ReadInt32()), 0);

		public string ReadString() => ReadString(_charset, ReadInt32());
		public string ReadString(int length) => ReadString(_charset, length);
		public string ReadString(Charset charset) => ReadString(charset, ReadInt32());

		public string ReadString(Charset charset, int length)
		{
			var buf = InternalReadBuffer(length, opaque: true);
			return charset.GetString(buf, 0, length);
		}

		public DateTime ReadDate() => TypeDecoder.DecodeDate(ReadInt32());
		public TimeSpan ReadTime() => TypeDecoder.DecodeTime(ReadInt32());
		public DateTime ReadDateTime()
		{
			var date = ReadDate();
			var time = ReadTime();
			return date.Add(time);
		}

		public decimal ReadDecimal(int type, int scale)
		{
			var value = 0m;
			switch (type & ~1)
			{
				case IscCodes.SQL_SHORT:
					value = TypeDecoder.DecodeDecimal(ReadInt16(), scale, type);
					break;

				case IscCodes.SQL_LONG:
					value = TypeDecoder.DecodeDecimal(ReadInt32(), scale, type);
					break;

				case IscCodes.SQL_QUAD:
				case IscCodes.SQL_INT64:
					value = TypeDecoder.DecodeDecimal(ReadInt64(), scale, type);
					break;

				case IscCodes.SQL_DOUBLE:
				case IscCodes.SQL_D_FLOAT:
					value = Convert.ToDecimal(ReadDouble());
					break;
			}
			return value;
		}

		public bool ReadBoolean()
		{
			return TypeDecoder.DecodeBoolean(InternalReadBuffer(1, opaque: true));
		}

		public Guid ReadGuid()
		{
			var buff = new byte[16];
			InternalReadBuffer(buff, 0, 16);
			return new Guid(buff);
		}

		public void Skip(int count)
		{
			while (count > 0)
			{
				var toRead = count > _innerBuffer.Length ? _innerBuffer.Length : count;
				InternalReadBuffer(_innerBuffer, 0, toRead);
				count -= toRead;
			}
		}

		public IscException ReadStatusVector()
		{
			IscException exception = null;
			bool eof = false;

			while (!eof)
			{
				int arg = ReadInt32();

				switch (arg)
				{
					case IscCodes.isc_arg_gds:
					default:
						int er = ReadInt32();
						if (er != 0)
						{
							if (exception == null)
							{
								exception = IscException.ForBuilding();
							}
							exception.Errors.Add(new IscError(arg, er));
						}
						break;

					case IscCodes.isc_arg_end:
						exception?.BuildExceptionData();
						eof = true;
						break;

					case IscCodes.isc_arg_interpreted:
					case IscCodes.isc_arg_string:
						exception.Errors.Add(new IscError(arg, ReadString()));
						break;

					case IscCodes.isc_arg_number:
						exception.Errors.Add(new IscError(arg, ReadInt32()));
						break;

					case IscCodes.isc_arg_sql_state:
						exception.Errors.Add(new IscError(arg, ReadString()));
						break;
				}
			}

			return exception;
		}

	}

	internal class XdrBinaryWriter
	{
		private readonly Stream _stream;
		private readonly Charset _charset;
		private readonly byte[] _innerBuffer = new byte[128];
		private static readonly byte[] _pad = new byte[4];
		private static byte[] _fill;

		private static byte[] Fill
		{
			get
			{
				var f = _fill;
				if (f == null)
				{
					f = new byte[32767];
					for (int i = 0; i < f.Length; i++)
					{
						f[i] = 32;
					}
					_fill = f;
				}

				return f;
			}
		}

		private void WritePadding(int length)
		{
			var padLen = (4 - length) & 3;
			if (padLen > 0)
				Write(_pad, 0, padLen);
		}

		public XdrBinaryWriter(Stream stream): this(stream, Charset.DefaultCharset) { }
		public XdrBinaryWriter(Stream stream, Charset charset)
		{
			_stream = stream;
			_charset = charset;
		}

		public Stream BaseStream => _stream;

		public void Write(byte[] buffer, int offset, int count)
		{
			if (buffer == null) return;
			_stream.Write(buffer, offset, count);
		}

		public void Write(ArraySegment<byte> buffer) => Write(buffer.Array, buffer.Offset, buffer.Count);

		private byte[] InternalGetBuffer(int count)
		{
			return count > _innerBuffer.Length ? new byte[count] : _innerBuffer;
		}

		public void WriteBuffer(ArraySegment<byte> buffer) => WriteBuffer(buffer.Array, buffer.Offset, buffer.Count);
		public void WriteBuffer(byte[] buffer) => WriteBuffer(buffer, 0, buffer == null ? 0 : buffer.Length);
		public void WriteBuffer(byte[] buffer, int length) => WriteBuffer(buffer, 0, length);
		public void WriteBuffer(byte[] buffer, int offset, int length)
		{
			Write(length);
			if (buffer != null && length > 0)
			{
				Write(buffer, offset, length);
				WritePadding(length);
			}
		}

		public void WriteOpaque(byte[] buffer) => WriteOpaque(buffer, buffer.Length);
		public void WriteOpaque(byte[] buffer, int length)
		{
			if (buffer != null && length > 0)
			{
				Write(buffer, 0, buffer.Length);
				Write(Fill, 0, length - buffer.Length);
				WritePadding(length);
			}
		}

		public void Write(byte value)
		{
			_stream.WriteByte(value);
		}
		public void Write(short value)
		{
			Write((int)value);
		}

		public void Write(int value)
		{
			value = IPAddress.NetworkToHostOrder(value);

			_innerBuffer[0] = (byte)value;
			_innerBuffer[1] = (byte)(value >> 8);
			_innerBuffer[2] = (byte)(value >> 16);
			_innerBuffer[3] = (byte)(value >> 24);
			Write(_innerBuffer, 0, 4);
		}

		public void Write(long value)
		{
			value = IPAddress.NetworkToHostOrder(value);

			_innerBuffer[0] = (byte)value;
			_innerBuffer[1] = (byte)(value >> 8);
			_innerBuffer[2] = (byte)(value >> 16);
			_innerBuffer[3] = (byte)(value >> 24);
			_innerBuffer[4] = (byte)(value >> 32);
			_innerBuffer[5] = (byte)(value >> 40);
			_innerBuffer[6] = (byte)(value >> 48);
			_innerBuffer[7] = (byte)(value >> 56);
			Write(_innerBuffer, 0, 8);
		}

		public void Write(float value)
		{
			var buffer = BitConverter.GetBytes(value);
			Write(BitConverter.ToInt32(buffer, 0));
		}

		public void Write(double value)
		{
			Write(BitConverter.DoubleToInt64Bits(value));
		}

		public void Write(decimal value, int type, int scale)
		{
			object numeric = TypeEncoder.EncodeDecimal(value, scale, type);
			switch (type & ~1)
			{
				case IscCodes.SQL_SHORT:
					Write((short)numeric);
					break;

				case IscCodes.SQL_LONG:
					Write((int)numeric);
					break;

				case IscCodes.SQL_QUAD:
				case IscCodes.SQL_INT64:
					Write((long)numeric);
					break;

				case IscCodes.SQL_DOUBLE:
				case IscCodes.SQL_D_FLOAT:
					Write((double)value);
					break;
			}
		}

		public void Write(string value) => Write(value, _charset);
		public void Write(string value, Charset charset)
		{
			var count = charset.GetBytesCount(value);
			Write(count);
			var buff = InternalGetBuffer(count);
			charset.GetBytes(value, 0, value.Length, buff, 0);
			Write(buff, 0, count);
			WritePadding(count);
		}

		public void WriteDate(DateTime value) => Write(TypeEncoder.EncodeDate(value));
		public void WriteTime(TimeSpan value) => Write(TypeEncoder.EncodeTime(value));
		public void Write(DateTime value)
		{
			WriteDate(value);
			WriteTime(TypeHelper.DateTimeToTimeSpan(value));
		}

		public void Write(bool value) => WriteOpaque(TypeEncoder.EncodeBoolean(value));
	}


	internal class DecompressionStream: Stream
	{
		// dont use MemoryStream because one is zeroing data on SetLength
		struct InputBuffer
		{
			public InputBuffer(byte[] buffer)
			{
				Buffer = buffer;
				Length = 0;
				Position = 0;
			}
			public byte[] Buffer { get; }
			public int Position { get; set; }
			public int Length { get; set; }
			public int Capacity => Buffer.Length;

			public int Read(byte[] buffer, int offset, int count)
			{
				var n = Length - Position;
				if (n > count) n = count;
				if (n == 0) return 0;
				Array.Copy(Buffer, Position, buffer, offset, n);
				Position += n;
				return n;
			}
		}

		private readonly Stream _stream;
		private readonly byte[] _streamBuffer;
		private InputBuffer _decompressedBuffer;
		private readonly ZlibCodec _inflate;

		public DecompressionStream(Stream stream, int inputBufferSize, int decompressedBufferSize)
		{
			_stream = stream;
			_streamBuffer = new byte[inputBufferSize];
			_decompressedBuffer = new InputBuffer(new byte[decompressedBufferSize]);
			_inflate = new Ionic.Zlib.ZlibCodec(Ionic.Zlib.CompressionMode.Decompress);
		}

		public override void Flush()
		{
			_decompressedBuffer.Length = 0;
			_decompressedBuffer.Position = 0;
		}

		public override long Seek(long offset, SeekOrigin origin)
		{
			throw new InvalidOperationException("DecompressionStream.Seek");
		}

		public override void SetLength(long value)
		{
			throw new InvalidOperationException("DecompressionStream.SetLength");
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			var n = _decompressedBuffer.Read(buffer, offset, count);
			if (n == count) return n;

			var readed = _stream.Read(_streamBuffer, 0, _streamBuffer.Length);

			_inflate.OutputBuffer = _decompressedBuffer.Buffer;
			_inflate.AvailableBytesOut = _decompressedBuffer.Capacity;
			_inflate.NextOut = 0;
			_inflate.InputBuffer = _streamBuffer;
			_inflate.AvailableBytesIn = readed;
			_inflate.NextIn = 0;
			var rc = _inflate.Inflate(Ionic.Zlib.FlushType.None);
			if (rc != Ionic.Zlib.ZlibConstants.Z_OK)
				throw new IOException($"Error '{rc}' while decompressing the data.");
			if (_inflate.AvailableBytesIn != 0)
				throw new IOException("Decompression buffer too small.");
			_decompressedBuffer.Position = 0;
			_decompressedBuffer.Length = _inflate.NextOut;

			n += _decompressedBuffer.Read(buffer, offset + n, count - n);
			return n;
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			_stream.Write(buffer, offset, count);
		}

		public override bool CanRead => _stream.CanRead;
		public override bool CanSeek => false;
		public override bool CanWrite => false;

		public override long Length
		{
			get { throw new InvalidOperationException("DecompressionStream.Length"); }
		}

		public override long Position
		{
			get { throw new InvalidOperationException("DecompressionStream.get_Position"); }
			set { throw new InvalidOperationException("DecompressionStream.set_Position"); }
		}

	}

}