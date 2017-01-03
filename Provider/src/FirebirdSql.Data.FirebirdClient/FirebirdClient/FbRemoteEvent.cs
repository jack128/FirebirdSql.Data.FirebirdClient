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
 *      Jiri Cincura (jiri@cincura.net)
 */

using System;
using System.Linq;
using System.Threading;
using FirebirdSql.Data.Common;

namespace FirebirdSql.Data.FirebirdClient
{
	public sealed class FbRemoteEvent
	{
		#region Events

		public event EventHandler<FbRemoteEventCountsEventArgs> RemoteEventCounts;
		public event EventHandler<FbRemoteEventErrorEventArgs> RemoteEventError;

		#endregion

		#region Fields

		private RemoteEvent _revent;
		private SynchronizationContext _synchronizationContext;

		#endregion

		#region Indexers

		public string this[int index] => _revent.Events[index];

		#endregion

		#region Properties

		public FbConnection Connection { get; }
		public int RemoteEventId => _revent?.RemoteId ?? -1;

		#endregion

		#region Constructors

		public FbRemoteEvent(FbConnection connection)
		{
			FbConnection.EnsureOpen(connection);

			Connection = connection;
			_revent = new RemoteEvent(connection.InnerConnection.Database);
			_revent.EventCountsCallback = OnRemoteEventCounts;
			_revent.EventErrorCallback = OnRemoteEventError;
			_synchronizationContext = SynchronizationContext.Current ?? new SynchronizationContext();
		}

		#endregion

		#region Methods

		public void QueueEvents(params string[] events)
		{
			if (_revent.Events.Any())
				throw new InvalidOperationException("Events are already running.");
			if (events == null)
				throw new ArgumentNullException(nameof(events));
			if (events.Length == 0)
				throw new ArgumentOutOfRangeException(nameof(events), "Need to provide at least one event.");
			if (events.Length > RemoteEvent.MaxEvents)
				throw new ArgumentOutOfRangeException(nameof(events), $"Maximum number of events is {RemoteEvent.MaxEvents}.");

			_revent.Events.AddRange(events);

			try
			{
				_revent.QueueEvents();
			}
			catch (IscException ex)
			{
				throw new FbException(ex.Message, ex);
			}
		}

		public void CancelEvents()
		{
			try
			{
				_revent.CancelEvents();
				_revent.Events.Clear();
			}
			catch (IscException ex)
			{
				throw new FbException(ex.Message, ex);
			}
		}

		#endregion

		#region Callbacks Handlers

		private void OnRemoteEventCounts(string name, int count)
		{
			var args = new FbRemoteEventCountsEventArgs(name, count);
			_synchronizationContext.Post(_ =>
			{
				RemoteEventCounts?.Invoke(this, args);
			}, null);
		}

		private void OnRemoteEventError(Exception error)
		{
			var args = new FbRemoteEventErrorEventArgs(error);
			_synchronizationContext.Post(_ =>
			{
				RemoteEventError?.Invoke(this, args);
			}, null);
		}

		#endregion
	}
}
