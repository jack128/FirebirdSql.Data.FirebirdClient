/*
 *  Firebird ADO.NET Data provider for .NET and Mono
 *
 *     The contents of this file are subject to the Initial
 *     Developer's Public License Version 1.0 (the "License");
 *     you may not use this file except in compliance with the
 *     License. You may obtain a copy of the License at
 *     http://www.firebirdsql.org/index.php?op=doc&id=idpl
 *
 *     Software distributed under the License is distributed on
 *     an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either
 *     express or implied.  See the License for the specific
 *     language governing rights and limitations under the License.
 *
 *  Copyright (c) 2002, 2007 Carlos Guzman Alvarez
 *  All Rights Reserved.
 *
 *  Contributors:
 *    Jiri Cincura (jiri@cincura.net)
 */

using System;
using System.Globalization;

namespace FirebirdSql.Data.Common
{

	internal abstract class DbValueBase
	{
		public abstract object Value { get; }

		#region Methods

		public abstract bool IsDBNull();

		public virtual string GetString() => Value?.ToString();
		public virtual char GetChar() => Convert.ToChar(Value, CultureInfo.CurrentCulture);
		public virtual bool GetBoolean() => Convert.ToBoolean(Value, CultureInfo.InvariantCulture);
		public virtual byte GetByte() => Convert.ToByte(Value, CultureInfo.InvariantCulture);
		public virtual short GetInt16() => Convert.ToInt16(Value, CultureInfo.InvariantCulture);
		public virtual int GetInt32() => Convert.ToInt32(Value, CultureInfo.InvariantCulture);
		public virtual long GetInt64() => Convert.ToInt64(Value, CultureInfo.InvariantCulture);
		public virtual decimal GetDecimal() => Convert.ToDecimal(Value, CultureInfo.InvariantCulture);
		public virtual float GetFloat() => Convert.ToSingle(Value, CultureInfo.InvariantCulture);
		public virtual double GetDouble() => Convert.ToDouble(Value, CultureInfo.InvariantCulture);

		public virtual Guid GetGuid()
		{
			if (Value is Guid)
			{
				return (Guid)Value;
			}

			var bytes = Value as byte[];
			if (bytes != null)
			{
				return new Guid(bytes);
			}

			throw new InvalidOperationException("Incorrect Guid value");
		}

		public virtual DateTime GetDateTime()
		{
			if (Value is TimeSpan)
				return new DateTime(0 * 10000L + 621355968000000000 + ((TimeSpan)Value).Ticks);
			if (Value is DateTimeOffset)
				return Convert.ToDateTime(((DateTimeOffset)Value).DateTime, CultureInfo.CurrentCulture.DateTimeFormat);
			return Convert.ToDateTime(Value, CultureInfo.CurrentCulture.DateTimeFormat);
		}

		#endregion

		public virtual Array GetArray()
		{
			throw new InvalidOperationException("Long value cannot be converted to array");
		}

		public virtual byte[] GetBinary()
		{
			throw new InvalidOperationException("Long value cannot be converted to binary");
		}
	}

	internal class NullDbValue : DbValueBase
	{
		public static readonly NullDbValue Instance = new NullDbValue();
		private NullDbValue() { }

		public override bool IsDBNull() => true;
		public override object Value => DBNull.Value;
		public override long GetInt64() => 0;
		public override byte GetByte() => 0;
		public override double GetDouble() => 0;
		public override float GetFloat() => 0;
		public override short GetInt16() => 0;
		public override int GetInt32() => 0;
		public override bool GetBoolean() => false;
		public override char GetChar() => (char)0;
		public override decimal GetDecimal() => 0;
	}

	internal class GuidDbValue : DbValueBase
	{
		private readonly Guid _value;

		public GuidDbValue(Guid value) { _value = value; }

		public override bool IsDBNull() => false;
		public override object Value => _value;
		public override Guid GetGuid() => _value;
	}

	internal class BooleanDbValue : DbValueBase
	{
		public static readonly BooleanDbValue True = new BooleanDbValue(true);
		public static readonly BooleanDbValue False = new BooleanDbValue(false);

		private static readonly object _trueBoxed = true;
		private static readonly object _falseBoxed = false;

		private readonly bool _value;
		private BooleanDbValue(bool value) { _value = value; }
		public override bool IsDBNull() => false;
		public override object Value => _value ? _trueBoxed : _falseBoxed;
		public override long GetInt64() => Convert.ToInt64(_value);
		public override byte GetByte() => Convert.ToByte(_value);
		public override double GetDouble() => Convert.ToDouble(_value);
		public override float GetFloat() => Convert.ToSingle(_value);
		public override short GetInt16() => Convert.ToInt16(_value);
		public override int GetInt32() => Convert.ToInt32(_value);
		public override bool GetBoolean() => _value;
		public override char GetChar() => Convert.ToChar(_value);
		public override decimal GetDecimal() => Convert.ToDecimal(_value);
	}


	internal class Int64DbValue : DbValueBase
	{
		private readonly long _value;
		public Int64DbValue(long value) { _value = value; }
		public override bool IsDBNull() => false;
		public override object Value => _value;
		public override long GetInt64() => _value;

		public override byte GetByte() => Convert.ToByte(_value);
		public override double GetDouble() => Convert.ToDouble(_value);
		public override float GetFloat() => Convert.ToSingle(_value);
		public override short GetInt16() => Convert.ToInt16(_value);
		public override int GetInt32() => Convert.ToInt32(_value);
		public override bool GetBoolean() => Convert.ToBoolean(_value);
		public override char GetChar() => Convert.ToChar(_value);
		public override decimal GetDecimal() => Convert.ToDecimal(_value);
	}

	internal class Int32DbValue : DbValueBase
	{
		private readonly int _value;
		public Int32DbValue(int value) { _value = value; }

		public override bool IsDBNull() => false;
		public override object Value => _value;
		public override long GetInt64() => _value;
		public override byte GetByte() => Convert.ToByte(_value);
		public override double GetDouble() => Convert.ToDouble(_value);
		public override float GetFloat() => Convert.ToSingle(_value);
		public override short GetInt16() => Convert.ToInt16(_value);
		public override int GetInt32() => _value;
		public override bool GetBoolean() => Convert.ToBoolean(_value);
		public override char GetChar() => Convert.ToChar(_value);
		public override decimal GetDecimal() => Convert.ToDecimal(_value);
	}

	internal class Int16DbValue : DbValueBase
	{
		private readonly short _value;
		public Int16DbValue(short value) { _value = value; }

		public override bool IsDBNull() => false;
		public override object Value => _value;
		public override long GetInt64() => _value;
		public override byte GetByte() => Convert.ToByte(_value);
		public override double GetDouble() => Convert.ToDouble(_value);
		public override float GetFloat() => Convert.ToSingle(_value);
		public override short GetInt16() => _value;
		public override int GetInt32() => _value;
		public override bool GetBoolean() => Convert.ToBoolean(_value);
		public override char GetChar() => Convert.ToChar(_value);
		public override decimal GetDecimal() => Convert.ToDecimal(_value);
	}
	internal class DecimalDbValue : DbValueBase
	{
		private readonly decimal _value;
		public DecimalDbValue(decimal value) { _value = value; }

		public override bool IsDBNull() => false;
		public override object Value => _value;
		public override long GetInt64() => Convert.ToInt64(_value);
		public override byte GetByte() => Convert.ToByte(_value);
		public override double GetDouble() => Convert.ToDouble(_value);
		public override float GetFloat() => Convert.ToSingle(_value);
		public override short GetInt16() => Convert.ToInt16(_value);
		public override int GetInt32() => Convert.ToInt32(_value);
		public override bool GetBoolean() => Convert.ToBoolean(_value);
		public override char GetChar() => Convert.ToChar(_value);
		public override decimal GetDecimal() => Convert.ToDecimal(_value);
	}
	internal class DoubleDbValue : DbValueBase
	{
		private readonly double _value;
		public DoubleDbValue(double value) { _value = value; }

		public override bool IsDBNull() => false;
		public override object Value => _value;
		public override long GetInt64() => Convert.ToInt64(_value);
		public override byte GetByte() => Convert.ToByte(_value);
		public override double GetDouble() => Convert.ToDouble(_value);
		public override float GetFloat() => Convert.ToSingle(_value);
		public override short GetInt16() => Convert.ToInt16(_value);
		public override int GetInt32() => Convert.ToInt32(_value);
		public override bool GetBoolean() => Convert.ToBoolean(_value);
		public override char GetChar() => Convert.ToChar(_value);
		public override decimal GetDecimal() => Convert.ToDecimal(_value);
	}
	internal class SingleDbValue : DbValueBase
	{
		private readonly float _value;
		public SingleDbValue(float value) { _value = value; }

		public override bool IsDBNull() => false;
		public override object Value => _value;
		public override long GetInt64() => Convert.ToInt64(_value);
		public override byte GetByte() => Convert.ToByte(_value);
		public override double GetDouble() => Convert.ToDouble(_value);
		public override float GetFloat() => Convert.ToSingle(_value);
		public override short GetInt16() => Convert.ToInt16(_value);
		public override int GetInt32() => Convert.ToInt32(_value);
		public override bool GetBoolean() => Convert.ToBoolean(_value);
		public override char GetChar() => Convert.ToChar(_value);
		public override decimal GetDecimal() => Convert.ToDecimal(_value);
	}
	internal class DateTimeDbValue : DbValueBase
	{
		private readonly DateTime _value;
		public DateTimeDbValue(DateTime value) { _value = value; }

		public override bool IsDBNull() => false;
		public override object Value => _value;
		public override long GetInt64() => Convert.ToInt64(_value);
		public override byte GetByte() => Convert.ToByte(_value);
		public override double GetDouble() => Convert.ToDouble(_value);
		public override float GetFloat() => Convert.ToSingle(_value);
		public override short GetInt16() => Convert.ToInt16(_value);
		public override int GetInt32() => Convert.ToInt32(_value);
		public override bool GetBoolean() => Convert.ToBoolean(_value);
		public override char GetChar() => Convert.ToChar(_value);
		public override decimal GetDecimal() => Convert.ToDecimal(_value);
		public override DateTime GetDateTime() => _value;
	}


	internal sealed class DbValue: DbValueBase
	{
		#region Fields

		private StatementBase _statement;
		private DbField _field;
		private object _value;

		#endregion

		#region Properties

		public DbField Field
		{
			get { return _field; }
		}

		public override object Value
		{
			get { return GetValue(); }
		}

		public void SetValue(object value)
		{
			_value = value;
		}

		#endregion

		#region Constructors

		public DbValue(DbField field, object value)
		{
			_field = field;
			_value = value ?? DBNull.Value;
		}

		public DbValue(StatementBase statement, DbField field)
		{
			_statement = statement;
			_field = field;
			_value = field.Value;
		}

		public DbValue(StatementBase statement, DbField field, object value)
		{
			_statement = statement;
			_field = field;
			_value = value ?? DBNull.Value;
		}

		#endregion

		#region Methods

		public override bool IsDBNull()
		{
			return TypeHelper.IsDBNull(_value);
		}

		public override string GetString()
		{
			if (Field.DbDataType == DbDataType.Text && _value is long)
			{
				_value = GetClobData((long)_value);
			}
			if (_value is byte[])
			{
				return Field.Charset.GetString((byte[])_value);
			}

			return _value.ToString();
		}

		public override char GetChar()
		{
			return Convert.ToChar(_value, CultureInfo.CurrentCulture);
		}

		public override bool GetBoolean()
		{
			return Convert.ToBoolean(_value, CultureInfo.InvariantCulture);
		}

		public override byte GetByte()
		{
			return Convert.ToByte(_value, CultureInfo.InvariantCulture);
		}

		public override short GetInt16()
		{
			return Convert.ToInt16(_value, CultureInfo.InvariantCulture);
		}

		public override int GetInt32()
		{
			return Convert.ToInt32(_value, CultureInfo.InvariantCulture);
		}

		public override long GetInt64()
		{
			return Convert.ToInt64(_value, CultureInfo.InvariantCulture);
		}

		public override decimal GetDecimal()
		{
			return Convert.ToDecimal(_value, CultureInfo.InvariantCulture);
		}

		public override float GetFloat()
		{
			return Convert.ToSingle(_value, CultureInfo.InvariantCulture);
		}

		public override Guid GetGuid()
		{
			if (Value is Guid)
			{
				return (Guid)Value;
			}
			else if (Value is byte[])
			{
				return new Guid((byte[])_value);
			}

			throw new InvalidOperationException("Incorrect Guid value");
		}

		public override double GetDouble()
		{
			return Convert.ToDouble(_value, CultureInfo.InvariantCulture);
		}

		public override DateTime GetDateTime()
		{
			if (_value is TimeSpan)
				return new DateTime(0 * 10000L + 621355968000000000 + ((TimeSpan)_value).Ticks);
			else if (_value is DateTimeOffset)
				return Convert.ToDateTime(((DateTimeOffset)_value).DateTime, CultureInfo.CurrentCulture.DateTimeFormat);
			else
				return Convert.ToDateTime(_value, CultureInfo.CurrentCulture.DateTimeFormat);
		}

		public override Array GetArray()
		{
			if (_value is long)
			{
				_value = GetArrayData((long)_value);
			}

			return (Array)_value;
		}

		public override byte[] GetBinary()
		{
			if (_value is long)
			{
				_value = GetBlobData((long)_value);
			}

			return (byte[])_value;
		}

		public int GetDate()
		{
			return TypeEncoder.EncodeDate(GetDateTime());
		}

		public int GetTime()
		{
			if (_value is TimeSpan)
				return TypeEncoder.EncodeTime((TimeSpan)_value);
			else
				return TypeEncoder.EncodeTime(TypeHelper.DateTimeToTimeSpan(GetDateTime()));
		}

		public byte[] GetBytes()
		{
			if (IsDBNull())
			{
				int length = _field.Length;

				if (Field.SqlType == IscCodes.SQL_VARYING)
				{
					// Add two bytes more for store	value length
					length += 2;
				}

				return new byte[length];
			}


			switch (Field.DbDataType)
			{
				case DbDataType.Char:
					{
						var buffer = new byte[Field.Length];
						byte[] bytes;

						if (Field.Charset.IsOctetsCharset)
						{
							bytes = GetBinary();
						}
						else
						{
							var svalue = GetString();

							if ((Field.Length % Field.Charset.BytesPerCharacter) == 0 &&
								svalue.Length > Field.CharCount)
							{
								throw IscException.ForErrorCodes(new[] { IscCodes.isc_arith_except, IscCodes.isc_string_truncation });
							}

							bytes = Field.Charset.GetBytes(svalue);
						}

						for (var i = 0; i < buffer.Length; i++)
						{
							buffer[i] = (byte)' ';
						}
						Buffer.BlockCopy(bytes, 0, buffer, 0, bytes.Length);
						return buffer;
					}

				case DbDataType.VarChar:
					{
						var buffer = new byte[Field.Length + 2];
						byte[] bytes;

						if (Field.Charset.IsOctetsCharset)
						{
							bytes = GetBinary();
						}
						else
						{
							var svalue = GetString();

							if ((Field.Length % Field.Charset.BytesPerCharacter) == 0 &&
								svalue.Length > Field.CharCount)
							{
								throw IscException.ForErrorCodes(new[] { IscCodes.isc_arith_except, IscCodes.isc_string_truncation });
							}

							bytes = Field.Charset.GetBytes(svalue);
						}

						Buffer.BlockCopy(BitConverter.GetBytes((short)bytes.Length), 0, buffer, 0, 2);
						Buffer.BlockCopy(bytes, 0, buffer, 2, bytes.Length);
						return buffer;
					}

				case DbDataType.Numeric:
				case DbDataType.Decimal:
					return GetNumericBytes();

				case DbDataType.SmallInt:
					return BitConverter.GetBytes(GetInt16());

				case DbDataType.Integer:
					return BitConverter.GetBytes(GetInt32());

				case DbDataType.Array:
				case DbDataType.Binary:
				case DbDataType.Text:
				case DbDataType.BigInt:
					return BitConverter.GetBytes(GetInt64());

				case DbDataType.Float:
					return BitConverter.GetBytes(GetFloat());

				case DbDataType.Double:
					return BitConverter.GetBytes(GetDouble());

				case DbDataType.Date:
					return BitConverter.GetBytes(TypeEncoder.EncodeDate(GetDateTime()));

				case DbDataType.Time:
					return BitConverter.GetBytes(GetTime());

				case DbDataType.TimeStamp:
					var dt = GetDateTime();
					var date = BitConverter.GetBytes(TypeEncoder.EncodeDate(dt));
					var time = BitConverter.GetBytes(TypeEncoder.EncodeTime(TypeHelper.DateTimeToTimeSpan(dt)));

					var result = new byte[8];

					Buffer.BlockCopy(date, 0, result, 0, date.Length);
					Buffer.BlockCopy(time, 0, result, 4, time.Length);

					return result;

				case DbDataType.Guid:
					return GetGuid().ToByteArray();

				case DbDataType.Boolean:
					return BitConverter.GetBytes(GetBoolean());

				default:
					throw TypeHelper.InvalidDataType((int)Field.DbDataType);
			}
		}

		private byte[] GetNumericBytes()
		{
			decimal value = GetDecimal();
			object numeric = TypeEncoder.EncodeDecimal(value, Field.NumericScale, Field.DataType);

			switch (_field.SqlType)
			{
				case IscCodes.SQL_SHORT:
					return BitConverter.GetBytes((short)numeric);

				case IscCodes.SQL_LONG:
					return BitConverter.GetBytes((int)numeric);

				case IscCodes.SQL_INT64:
				case IscCodes.SQL_QUAD:
					return BitConverter.GetBytes((long)numeric);

				case IscCodes.SQL_DOUBLE:
					return BitConverter.GetBytes(GetDouble());

				default:
					return null;
			}
		}

		#endregion

		#region Private Methods

		private object GetValue()
		{
			if (IsDBNull())
			{
				return DBNull.Value;
			}

			switch (_field.DbDataType)
			{
				case DbDataType.Text:
					if (_statement == null)
					{
						return GetInt64();
					}
					else
					{
						return GetString();
					}

				case DbDataType.Binary:
					if (_statement == null)
					{
						return GetInt64();
					}
					else
					{
						return GetBinary();
					}

				case DbDataType.Array:
					if (_statement == null)
					{
						return GetInt64();
					}
					else
					{
						return GetArray();
					}

				default:
					return _value;
			}
		}

		private string GetClobData(long blobId)
		{
			BlobBase clob = _statement.CreateBlob(blobId);

			return clob.ReadString();
		}

		private byte[] GetBlobData(long blobId)
		{
			BlobBase blob = _statement.CreateBlob(blobId);

			return blob.Read();
		}

		private Array GetArrayData(long handle)
		{
			if (_field.ArrayHandle == null)
			{
				_field.ArrayHandle = _statement.CreateArray(handle, Field.Relation, Field.Name);
			}

			ArrayBase gdsArray = _statement.CreateArray(_field.ArrayHandle.Descriptor);

			gdsArray.Handle = handle;
			gdsArray.DB = _statement.Database;
			gdsArray.Transaction = _statement.Transaction;

			return gdsArray.Read();
		}

		#endregion
	}
}
