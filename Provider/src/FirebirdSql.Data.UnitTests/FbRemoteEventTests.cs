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
 *  Copyright (c) 2017 Jiri Cincura (jiri@cincura.net)
 *  All Rights Reserved.
 */

using System;
using System.Data;
using System.Threading;
using FirebirdSql.Data.FirebirdClient;
using NUnit.Framework;

namespace FirebirdSql.Data.UnitTests
{
	[FbTestFixture(FbServerType.Default, false)]
	[FbTestFixture(FbServerType.Default, true)]
	[FbTestFixture(FbServerType.Embedded, default(bool))]
	public class FbRemoteEventTests : TestsBase
	{
		public FbRemoteEventTests(FbServerType serverType, bool compression)
			: base(serverType, compression)
		{ }

		[Test]
		public void EventComesBackTest()
		{
			var triggered = false;
			using (var @event = new FbRemoteEvent(Connection.ConnectionString))
			{
				@event.RemoteEventCounts += (sender, e) =>
				{
					triggered = e.Name == "test" && e.Counts == 1;
				};
				@event.QueueEvents("test");
				using (var cmd = Connection.CreateCommand())
				{
					cmd.CommandText = "execute block as begin post_event 'test'; end";
					cmd.ExecuteNonQuery();
				}
				Thread.Sleep(200);
				Assert.IsTrue(triggered);
			}
		}
	}
}
