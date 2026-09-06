using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Client
{
    public static class IConnectionExtensions
    {
        /// <summary>
        /// Asynchronously close this connection and all its channels.
        /// </summary>
        /// <remarks>
        /// Note that all active channels and sessions will be closed if this method is called.
        /// It waits 30 seconds for the in-progress close operation to complete and throws if that
        /// elapses: an <see cref="OperationCanceledException"/> rather than
        /// <see cref="IOException"/>, which signals a socket closed unexpectedly. Catch
        /// <see cref="OperationCanceledException"/> rather than its
        /// <see cref="System.Threading.Tasks.TaskCanceledException"/> subclass: which of the two is
        /// thrown depends on the build of this library your application resolves, not on the runtime
        /// it executes on, and only the net8.0 build throws the subclass. Note that a
        /// connection returned by <see cref="ConnectionFactory"/> with automatic recovery enabled,
        /// the default, first stops its recovery loop on a separate budget of
        /// <see cref="ConnectionFactory.RequestedConnectionTimeout"/>, so the total time can
        /// exceed 30 seconds. On a connection that is already closed this does nothing when
        /// automatic recovery is enabled, and throws <see cref="Exceptions.AlreadyClosedException"/>
        /// when it is not.
        /// </remarks>
        public static Task CloseAsync(this IConnection connection, CancellationToken cancellationToken = default)
        {
            return connection.CloseAsync(Constants.ReplySuccess, "Goodbye", InternalConstants.DefaultConnectionCloseTimeout, false,
                cancellationToken);
        }

        /// <summary>
        /// Asynchronously close this connection and all its channels.
        /// </summary>
        /// <remarks>
        /// The method behaves in the same way as <see cref="CloseAsync(IConnection, CancellationToken)"/>, with the only
        /// difference that the connection is closed with the given connection close code and message.
        /// <para>
        /// The close code (See under "Reply Codes" in the AMQP specification).
        /// </para>
        /// <para>
        /// A message indicating the reason for closing the connection.
        /// </para>
        /// </remarks>
        public static Task CloseAsync(this IConnection connection, ushort reasonCode, string reasonText,
            CancellationToken cancellationToken = default)
        {
            return connection.CloseAsync(reasonCode, reasonText, InternalConstants.DefaultConnectionCloseTimeout, false,
                cancellationToken);
        }

        /// <summary>
        /// Asynchronously close this connection and all its channels
        /// and wait with a timeout for all the in-progress close operations to complete.
        /// </summary>
        /// <remarks>
        /// Note that all active channels and sessions will be
        /// closed if this method is called. It will wait for the in-progress
        /// close operation to complete with a timeout. On a connection that is already closed this
        /// does nothing when automatic recovery is enabled, the default, and throws
        /// <see cref="Exceptions.AlreadyClosedException"/> when it is not.
        /// It can also throw <see cref="IOException"/> when socket was closed unexpectedly.
        /// If the timeout is reached the wait ends and the connection is torn down on a best-effort
        /// basis. Note that a connection returned by <see cref="ConnectionFactory"/> with automatic
        /// recovery enabled first stops its recovery loop on a budget of
        /// <see cref="ConnectionFactory.RequestedConnectionTimeout"/>, so the total time can exceed
        /// <paramref name="timeout"/>.
        /// <para>
        /// To wait infinitely for the close operations to complete use <see cref="System.Threading.Timeout.InfiniteTimeSpan"/>.
        /// </para>
        /// <para>
        /// A finite timeout shorter than 30 seconds is raised to 30 seconds, because the
        /// timeout also bounds the close handshake itself and cutting that short leaves the
        /// connection only partly shut down. Use <see cref="System.Threading.Timeout.InfiniteTimeSpan"/>
        /// to wait without a bound.
        /// </para>
        /// <para>
        /// A value too large for the timer, including <see cref="TimeSpan.MaxValue"/>, is clamped to
        /// the largest supported bound rather than throwing. That limit depends on which build of
        /// this library your application resolves, not on the runtime it executes on: roughly 24.86
        /// days for the netstandard2.0 build, which is what .NET Framework and .NET versions before
        /// 8 load, and roughly 49.7 days for the net8.0 build.
        /// </para>
        /// <para>
        /// <see cref="System.Threading.Timeout.InfiniteTimeSpan"/> waits without any bound, and
        /// nothing else can end that wait: no timer is armed, and a
        /// <see cref="CancellationToken"/> passed to the underlying
        /// <see cref="IConnection.CloseAsync(ushort, string, TimeSpan, bool, CancellationToken)"/>
        /// is deliberately ignored while the underlying connection is open, so that a close already
        /// under way is not truncated. On a connection with automatic recovery enabled, the default,
        /// the token is still observed while the recovery loop is stopped, which happens before
        /// that.
        /// </para>
        /// <para>
        /// The bound this timeout removes covers the whole close, not only the wait for the peer's
        /// reply. Sending <c>connection.close</c> is bounded by the same value, and writes queue
        /// into a bounded buffer, so a peer that has stopped reading (a stalled or zero-window
        /// connection) can park an unbounded close indefinitely before any reply is even expected.
        /// Use <see cref="System.Threading.Timeout.InfiniteTimeSpan"/> only where that is
        /// acceptable; prefer a large finite timeout otherwise.
        /// </para>
        /// </remarks>
        public static Task CloseAsync(this IConnection connection, TimeSpan timeout)
        {
            return connection.CloseAsync(Constants.ReplySuccess, "Goodbye", timeout, false,
                CancellationToken.None);
        }

        /// <summary>
        /// Asynchronously close this connection and all its channels
        /// and wait with a timeout for all the in-progress close operations to complete.
        /// </summary>
        /// <remarks>
        /// The method behaves in the same way as <see cref="CloseAsync(IConnection,TimeSpan)"/>, with the only
        /// difference that the connection is closed with the given connection close code and message.
        /// <para>
        /// The close code (See under "Reply Codes" in the AMQP 0-9-1 specification).
        /// </para>
        /// <para>
        /// A message indicating the reason for closing the connection.
        /// </para>
        /// <para>
        /// Operation timeout.
        /// </para>
        /// </remarks>
        public static Task CloseAsync(this IConnection connection, ushort reasonCode, string reasonText, TimeSpan timeout)
        {
            return connection.CloseAsync(reasonCode, reasonText, timeout, false,
                CancellationToken.None);
        }

        /// <summary>
        /// Asynchronously abort this connection and all its channels.
        /// </summary>
        /// <remarks>
        /// Note that all active channels and sessions will be closed if this method is called.
        /// In comparison to normal <see cref="CloseAsync(IConnection, CancellationToken)"/> method, <see cref="AbortAsync(IConnection, CancellationToken)"/> will not throw
        /// <see cref="IOException"/> during closing connection.
        /// This method waits 5 seconds for the in-progress close operation to complete and then
        /// attempts to close the socket, and unlike a graceful close it does not rethrow when that
        /// wait elapses. Note that a connection returned by <see cref="ConnectionFactory"/> with
        /// automatic recovery enabled, the default, first stops its recovery loop on a separate
        /// budget of <see cref="ConnectionFactory.RequestedConnectionTimeout"/>, so the total time
        /// can exceed 5 seconds.
        /// </remarks>
        public static Task AbortAsync(this IConnection connection)
        {
            return connection.CloseAsync(Constants.ReplySuccess, "Connection close forced",
                InternalConstants.DefaultConnectionAbortTimeout, true, default);
        }

        /// <summary>
        /// Asynchronously abort this connection and all its channels.
        /// </summary>
        /// <remarks>
        /// Note that all active channels and sessions will be closed if this method is called.
        /// In comparison to normal <see cref="CloseAsync(IConnection, CancellationToken)"/> method, <see cref="AbortAsync(IConnection, CancellationToken)"/> will not throw
        /// <see cref="IOException"/> during closing connection.
        /// This method waits 5 seconds for the in-progress close operation to complete and then
        /// attempts to close the socket, and unlike a graceful close it does not rethrow when that
        /// wait elapses. Note that a connection returned by <see cref="ConnectionFactory"/> with
        /// automatic recovery enabled, the default, first stops its recovery loop on a separate
        /// budget of <see cref="ConnectionFactory.RequestedConnectionTimeout"/>, so the total time
        /// can exceed 5 seconds.
        /// </remarks>
        public static Task AbortAsync(this IConnection connection, CancellationToken cancellationToken = default)
        {
            return connection.CloseAsync(Constants.ReplySuccess, "Connection close forced",
                InternalConstants.DefaultConnectionAbortTimeout, true, cancellationToken);
        }

        /// <summary>
        /// Asynchronously abort this connection and all its channels.
        /// </summary>
        /// <remarks>
        /// The method behaves in the same way as <see cref="AbortAsync(IConnection, CancellationToken)"/>, with the only
        /// difference that the connection is closed with the given connection close code and message.
        /// <para>
        /// The close code (See under "Reply Codes" in the AMQP 0-9-1 specification)
        /// </para>
        /// <para>
        /// A message indicating the reason for closing the connection
        /// </para>
        /// </remarks>
        public static Task AbortAsync(this IConnection connection, ushort reasonCode, string reasonText)
        {
            return connection.CloseAsync(reasonCode, reasonText,
                InternalConstants.DefaultConnectionAbortTimeout, true, default);
        }

        /// <summary>
        /// Asynchronously abort this connection and all its channels.
        /// </summary>
        /// <remarks>
        /// The method behaves in the same way as <see cref="AbortAsync(IConnection, CancellationToken)"/>, with the only
        /// difference that the connection is closed with the given connection close code and message.
        /// <para>
        /// The close code (See under "Reply Codes" in the AMQP 0-9-1 specification)
        /// </para>
        /// <para>
        /// A message indicating the reason for closing the connection
        /// </para>
        /// </remarks>
        public static Task AbortAsync(this IConnection connection, ushort reasonCode, string reasonText, CancellationToken cancellationToken = default)
        {
            return connection.CloseAsync(reasonCode, reasonText,
                InternalConstants.DefaultConnectionAbortTimeout, true, cancellationToken);
        }

        /// <summary>
        /// Asynchronously abort this connection and all its channels and wait with a
        /// timeout for all the in-progress close operations to complete.
        /// </summary>
        /// <remarks>
        /// This method, behaves in a similar way as method <see cref="AbortAsync(IConnection, CancellationToken)"/> with the
        /// only difference that it explicitly specifies a timeout given
        /// for all the in-progress close operations to complete.
        /// If timeout is reached and the close operations haven't finished, then socket is forced to close.
        /// <para>
        /// An abort is always bounded, so unlike <see cref="CloseAsync(IConnection,TimeSpan)"/>
        /// it does not honour <see cref="Timeout.InfiniteTimeSpan"/>. An abort's wait is bounded by
        /// this timeout alone, because the caller's cancellation token is deliberately neutralized
        /// on an open connection, so an unbounded abort would have nothing left that could end it -
        /// it could never return, which defeats the best-effort, never-throw contract that abort
        /// exists to provide. A timeout shorter than 5 seconds, or an
        /// unbounded one, is resolved to 5 seconds, because the timeout also bounds the close
        /// handshake itself and cutting that short leaves the connection only partly shut down. A
        /// finite value above 5 seconds is honoured as given, however large, after being clamped to
        /// the largest bound the timer supports.
        /// </para>
        /// </remarks>
        public static Task AbortAsync(this IConnection connection, TimeSpan timeout)
        {
            return connection.CloseAsync(Constants.ReplySuccess, "Connection close forced", timeout, true,
                CancellationToken.None);
        }

        /// <summary>
        /// Asynchronously abort this connection and all its channels and wait with a
        /// timeout for all the in-progress close operations to complete.
        /// </summary>
        /// <remarks>
        /// The method behaves in the same way as <see cref="AbortAsync(IConnection,TimeSpan)"/>, with the only
        /// difference that the connection is closed with the given connection close code and message.
        /// <para>
        /// The close code (See under "Reply Codes" in the AMQP 0-9-1 specification).
        /// </para>
        /// <para>
        /// A message indicating the reason for closing the connection.
        /// </para>
        /// </remarks>
        public static Task AbortAsync(this IConnection connection, ushort reasonCode, string reasonText, TimeSpan timeout)
        {
            return connection.CloseAsync(reasonCode, reasonText, timeout, true,
                CancellationToken.None);
        }
    }
}
