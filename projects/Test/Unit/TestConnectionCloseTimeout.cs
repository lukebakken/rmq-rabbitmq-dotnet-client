// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
//
// The APL v2.0:
//
//---------------------------------------------------------------------------
//   Copyright (c) 2007-2026 Broadcom. All Rights Reserved.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       https://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//---------------------------------------------------------------------------
//
// The MPL v2.0:
//
//---------------------------------------------------------------------------
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
//  Copyright (c) 2007-2026 Broadcom. All Rights Reserved.
//---------------------------------------------------------------------------

using System;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Impl;
using Xunit;

namespace Test.Unit
{
    /// <summary>
    /// rabbitmq/rabbitmq-dotnet-client#1973
    ///
    /// The floors applied to a caller-supplied close timeout are deliberate: the
    /// timeout feeds the same linked <see cref="CancellationTokenSource"/> as the
    /// caller's own token, so a zero or very small value would cancel the close
    /// handshake itself rather than bounding the wait for it, which is the
    /// <see cref="ObjectDisposedException"/> of #1802.
    ///
    /// <see cref="Timeout.InfiniteTimeSpan"/> is the exception. It means "take as
    /// long as needed", so it cannot cause that truncation, but because it is
    /// negative it compared as less than both floors and was silently lowered to a
    /// finite 30 seconds, making the documented infinite wait unreachable.
    ///
    /// These cases are asserted against the timeout resolution directly because the
    /// difference is otherwise unobservable: a healthy connection closes in roughly
    /// 175ms, so no close timeout is ever reached and the clamp leaves no trace.
    /// That is exactly why this went unnoticed, and why an integration test here
    /// would be vacuous.
    /// </summary>
    public class TestConnectionCloseTimeout
    {
        [Fact]
        public void InfiniteTimeSpanIsNotLoweredToTheCloseFloor_GH1973()
        {
            Assert.Equal(Timeout.InfiniteTimeSpan,
                Connection.ResolveCloseTimeout(Timeout.InfiniteTimeSpan, abort: false));
        }

        [Fact]
        public void AbortStaysBoundedWhenGivenInfiniteTimeSpan_GH1973()
        {
            /*
             * An abort's wait on the main loop uses the timeout alone, with the caller's
             * token deliberately neutralized, so an unbounded abort would make the forced
             * socket close unreachable and could hang for good when the main loop is
             * stranded. Abort therefore keeps its floor.
             */
            Assert.Equal(InternalConstants.DefaultConnectionAbortTimeout,
                Connection.ResolveCloseTimeout(Timeout.InfiniteTimeSpan, abort: true));
        }

        [Theory]
        [InlineData(-50000000)] // -5s:       rejected by CancellationTokenSource
        [InlineData(-20000)]    // -2ms:      rejected by CancellationTokenSource
        [InlineData(-10001)]    // -1.0001ms: ACCEPTED, and never cancels
        [InlineData(-9999)]     // -0.9999ms: ACCEPTED, and cancels immediately
        public void NegativeTimeoutOtherThanInfiniteIsRaisedToTheFloor_GH1973(long ticks)
        {
            /*
             * The exemption must match Timeout.InfiniteTimeSpan exactly and no other negative
             * value, because the floors are what keep every other negative away from the
             * CancellationTokenSource. Loosening the check to `timeout < TimeSpan.Zero`, or to
             * `(long)timeout.TotalMilliseconds == -1`, must fail here.
             *
             * The cases are chosen from measured constructor behaviour, not assumed. The
             * constructor validates (long)delay.TotalMilliseconds >= -1, and that cast truncates
             * toward zero, which splits the sub-2ms negatives into two regimes with opposite
             * hazards:
             *
             *   (-2ms, -1ms]  truncates to -1  -> accepted, timer never armed, never cancels.
             *                                     A silent unbounded close, the worse of the two.
             *   (-1ms, 0)     truncates to 0   -> accepted, cancels immediately, truncating the
             *                                     handshake as in #1802 with no exception to flag it.
             *
             * Anything at or below -2ms throws ArgumentOutOfRangeException instead, which is why
             * the first two cases alone would not cover the reachable hazards - they only exercise
             * values the constructor already rejects loudly. Ticks rather than milliseconds
             * because an int millisecond parameter cannot express -1.0001ms.
             */
            TimeSpan timeout = TimeSpan.FromTicks(ticks);
            Assert.NotEqual(Timeout.InfiniteTimeSpan, timeout);

            Assert.Equal(InternalConstants.DefaultConnectionCloseTimeout,
                Connection.ResolveCloseTimeout(timeout, abort: false));
            Assert.Equal(InternalConstants.DefaultConnectionAbortTimeout,
                Connection.ResolveCloseTimeout(timeout, abort: true));
        }

        [Fact]
        public void OverLargeCloseTimeoutIsClampedRatherThanThrowing_GH1973()
        {
            /*
             * CancellationTokenSource rejects a delay above its ceiling, so passing a larger value
             * through would throw out of its constructor before the close reason is set, leaving
             * the connection fully open. Such a value means "as long as possible", so it is clamped
             * to the ceiling. It is deliberately NOT promoted to InfiniteTimeSpan: that would turn
             * a bounded wait into one nothing can end, and would make the abort branch
             * non-monotonic.
             */
            Assert.Equal(Connection.s_maxCancellationTokenSourceDelay,
                Connection.ResolveCloseTimeout(TimeSpan.MaxValue, abort: false));
            Assert.Equal(Connection.s_maxCancellationTokenSourceDelay,
                Connection.ResolveCloseTimeout(TimeSpan.FromDays(60), abort: false));

            // An abort honours a large finite value as given, once clamped; only an unbounded or
            // too-small request resolves to the 5 second floor.
            Assert.Equal(Connection.s_maxCancellationTokenSourceDelay,
                Connection.ResolveCloseTimeout(TimeSpan.MaxValue, abort: true));
        }

        [Fact]
        public void ResolutionIsMonotonicAcrossTheCeiling_GH1973()
        {
            /*
             * Asking for more time must never yield less. When an over-large value was promoted to
             * InfiniteTimeSpan, an abort of 49 days resolved to 49 days while 50 days resolved to
             * the 5 second floor: a 5-order-of-magnitude reversal from one extra day.
             */
            TimeSpan ceiling = Connection.s_maxCancellationTokenSourceDelay;
            TimeSpan justUnder = ceiling - TimeSpan.FromDays(1);

            foreach (bool abort in new[] { false, true })
            {
                Assert.True(Connection.ResolveCloseTimeout(ceiling + TimeSpan.FromDays(1), abort)
                    >= Connection.ResolveCloseTimeout(justUnder, abort),
                    $"resolution went backwards across the ceiling (abort: {abort})");
            }
        }

        [Fact]
        public void TheCeilingItselfIsAcceptedAndOneTickAboveIsClamped_GH1973()
        {
            /*
             * The guard is `timeout > ceiling`, so the ceiling itself must pass through and a value
             * above it must clamp.
             *
             * Note what these two assertions alone cannot catch, because every comparison here is
             * against the ceiling constant itself: lowering the ceiling leaves them green. Mutating
             * the net8.0 ceiling from uint.MaxValue - 1 ms (~49.71 days) down to int.MaxValue ms
             * (~24.86 days) halves the honoured bound and contradicts this library's own public
             * documentation, yet the whole suite still passes. Nor can `>` mutated to `>=` be
             * caught: the clamp assigns the ceiling, so both spellings return the same value.
             *
             * The maximality assertion below is what closes that gap, and it is the reason to
             * express the step in milliseconds rather than ticks. `ceiling + FromTicks(1)` is
             * itself accepted by the constructor, because (long)TotalMilliseconds truncates it back
             * to the same millisecond, so a one-tick step can never reproduce the
             * ArgumentOutOfRangeException-before-shutdown bug this file exists for.
             */
            TimeSpan ceiling = Connection.s_maxCancellationTokenSourceDelay;
            TimeSpan justOver = ceiling + TimeSpan.FromMilliseconds(1);

            Assert.Equal(ceiling, Connection.ResolveCloseTimeout(ceiling, abort: false));
            Assert.Equal(ceiling, Connection.ResolveCloseTimeout(justOver, abort: false));

            // The ceiling must be the largest value CancellationTokenSource accepts on this
            // runtime: accepted at the ceiling, rejected one millisecond above it. The second half
            // is what fails if the ceiling is ever set below the runtime's real limit.
            using (var cts = new CancellationTokenSource(ceiling))
            {
                Assert.NotEqual(default, cts.Token);
            }

            Assert.Throws<ArgumentOutOfRangeException>(() => new CancellationTokenSource(justOver));
        }

        [Fact]
        public void ResolvedTimeoutIsAlwaysAcceptedByCancellationTokenSource_GH1973()
        {
            /*
             * The resolved value is handed straight to a CancellationTokenSource, so every
             * resolution must be constructible. This is the invariant that the over-large
             * and negative handling above exists to maintain.
             */
            TimeSpan[] inputs =
            {
                Timeout.InfiniteTimeSpan, TimeSpan.Zero, TimeSpan.FromSeconds(6),
                TimeSpan.FromSeconds(60), TimeSpan.FromMilliseconds(-2), TimeSpan.FromTicks(-10001),
                // FromDays(30) is above the .NET Framework CancellationTokenSource limit
                // (~24.86 days) but below the modern .NET limit, so it is what catches a
                // miscalibrated s_maxCancellationTokenSourceDelay on net472. See #1973.
                TimeSpan.FromDays(30),
                TimeSpan.FromDays(60), TimeSpan.MaxValue, TimeSpan.MinValue
            };

            foreach (TimeSpan input in inputs)
            {
                foreach (bool abort in new[] { false, true })
                {
                    TimeSpan resolved = Connection.ResolveCloseTimeout(input, abort);

                    /*
                     * Assert on the resolved value, not on cts.IsCancellationRequested. On .NET
                     * Framework a zero delay arms a timer rather than completing the source
                     * immediately, so reading the flag there is a race that reports false even for
                     * a resolution that cancels microseconds later. A resolution must be either
                     * unbounded or strictly positive.
                     */
                    Assert.True(resolved == Timeout.InfiniteTimeSpan || resolved > TimeSpan.Zero,
                        $"resolved timeout {resolved} for input {input} (abort: {abort}) would " +
                        "cancel the close handshake rather than bound the wait for it");

                    using var cts = new CancellationTokenSource(resolved);
                }
            }
        }

        [Theory]
        [InlineData(0)]
        [InlineData(6)]
        [InlineData(29)]
        public void SmallCloseTimeoutIsRaisedToTheCloseFloor_GH1973(int seconds)
        {
            Assert.Equal(InternalConstants.DefaultConnectionCloseTimeout,
                Connection.ResolveCloseTimeout(TimeSpan.FromSeconds(seconds), abort: false));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(4)]
        public void SmallAbortTimeoutIsRaisedToTheAbortFloor_GH1973(int seconds)
        {
            Assert.Equal(InternalConstants.DefaultConnectionAbortTimeout,
                Connection.ResolveCloseTimeout(TimeSpan.FromSeconds(seconds), abort: true));
        }

        [Fact]
        public void TimeoutAboveTheFloorIsUsedAsGiven_GH1973()
        {
            TimeSpan timeout = TimeSpan.FromSeconds(60);

            Assert.Equal(timeout, Connection.ResolveCloseTimeout(timeout, abort: false));
            Assert.Equal(timeout, Connection.ResolveCloseTimeout(timeout, abort: true));
        }
    }
}
