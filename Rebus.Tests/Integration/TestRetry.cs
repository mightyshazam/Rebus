﻿using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Extensions;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Retry.Simple;
using Rebus.Routing.TypeBased;
using Rebus.Tests.Extensions;
using Rebus.Tests.Transport.Msmq;
using Rebus.Transport.Msmq;

namespace Rebus.Tests.Integration
{
    [TestFixture]
    public class TestRetry : FixtureBase
    {
        static readonly string InputQueueName = MsmqHelper.QueueName(string.Format("test.retries.input@{0}", Environment.MachineName));

        BuiltinHandlerActivator _handlerActivator;
        IBus _bus;

        void InitializeBus(int numberOfRetries)
        {
            _handlerActivator = new BuiltinHandlerActivator();

            _bus = Configure.With(_handlerActivator)
                .Logging(l => l.Console(minLevel: LogLevel.Warn))
                .Transport(t => t.UseMsmq(InputQueueName))
                .Routing(r => r.TypeBased().Map<string>(InputQueueName))
                .Options(o => o.SimpleRetryStrategy(maxDeliveryAttempts: numberOfRetries))
                .Start();

            TrackDisposable(_bus);
        }

        protected override void TearDown()
        {
            MsmqUtil.Delete(InputQueueName);
            MsmqUtil.Delete(SimpleRetryStrategySettings.DefaultErrorQueueName);
        }

        [Test]
        public async Task ItWorks()
        {
            const int numberOfRetries = 5;

            InitializeBus(numberOfRetries);

            var attemptedDeliveries = 0;

            _handlerActivator.Handle<string>(async _ =>
            {
                Interlocked.Increment(ref attemptedDeliveries);
                throw new ApplicationException("omgwtf!");
            });

            await _bus.Send("hej");

            using (var errorQueue = new MsmqTransport(SimpleRetryStrategySettings.DefaultErrorQueueName))
            {
                var failedMessage = await errorQueue.AwaitReceive();

                Assert.That(attemptedDeliveries, Is.EqualTo(numberOfRetries));
                Assert.That(failedMessage.Headers.GetValue(Headers.ErrorDetails), Contains.Substring("omgwtf!"));
                Assert.That(failedMessage.Headers.GetValue(Headers.SourceQueue), Is.EqualTo(InputQueueName));
            }
        }

        [TestCase(1)]
        [TestCase(2)]
        [TestCase(5)]
        [TestCase(40)]
        [TestCase(70)]
        [TestCase(90)]
        public async Task CanConfigureNumberOfRetries(int numberOfRetries)
        {
            InitializeBus(numberOfRetries);

            var attemptedDeliveries = 0;

            _handlerActivator.Handle<string>(async _ =>
            {
                Interlocked.Increment(ref attemptedDeliveries);
                throw new ApplicationException("omgwtf!");
            });

            await _bus.Send("hej");

            using (var errorQueue = new MsmqTransport(SimpleRetryStrategySettings.DefaultErrorQueueName))
            {
                var expectedNumberOfAttemptedDeliveries = numberOfRetries;

                await errorQueue.AwaitReceive(1 + numberOfRetries / 10.0);

                Assert.That(attemptedDeliveries, Is.EqualTo(expectedNumberOfAttemptedDeliveries));
            }
        }
    }
}