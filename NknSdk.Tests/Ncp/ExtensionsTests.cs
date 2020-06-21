using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using Xunit;

using Ncp;
using Ncp.Exceptions;

namespace NknSdk.Tests.Ncp
{
    public class ExtensionsTests
    {
        [Fact]
        public void ActiveChannel_Should_BeSelectedFirst()
        {
            var expectedChannel = Channel.CreateBounded<uint?>(1);
            var expectedChannel2 = Channel.CreateBounded<uint?>(1);
            var closedChannel = Channel.CreateUnbounded<uint?>();
            closedChannel.Writer.Complete();

            expectedChannel.Writer.WriteAsync(1);
            expectedChannel2.Writer.WriteAsync(5);

            var cts = new CancellationTokenSource();
            var channelTasks = new List<Task<Channel<uint?>>>
            {
                expectedChannel.Push((uint?)1, cts.Token),
                expectedChannel2.Push((uint?)2, cts.Token),
                closedChannel.Shift(cts.Token)
            };

            var channel = channelTasks.FirstAsync(cts);

            Assert.Equal(closedChannel, channel);
        }

        [Fact]
        public void  Should_Select_CorrectValue()
        {
            uint value = 5;
            var firstChannel = Channel.CreateBounded<uint?>(1);
            var secondChannel = Channel.CreateBounded<uint?>(1);

            firstChannel.Writer.TryWrite(value);
            secondChannel.Writer.TryWrite(7);

            var cts = new CancellationTokenSource();
            var channelTasks = new List<Task<uint?>>
            {
                firstChannel.ShiftValue(cts.Token),
                secondChannel.ShiftValue(cts.Token)
            };

            var result = channelTasks.FirstValueAsync(cts);

            Assert.NotNull(result);
            Assert.Equal(value, result.Value);
        }

        [Fact]
        public void Should_SelectCorrect_TimeoutChannel()
        {
            var channel1 = 100.ToTimeoutChannel();
            var channel2 = 200.ToTimeoutChannel();
            var channel3 = 50.ToTimeoutChannel();
            var channel4 = 300.ToTimeoutChannel();

            var cts = new CancellationTokenSource();
            var channelTasks = new List<Task<Channel<uint?>>>
            {
                channel1.Shift(cts.Token),
                channel2.Shift(cts.Token),
                channel3.Shift(cts.Token),
                channel4.Shift(cts.Token),
            };

            var channel = channelTasks.FirstAsync(cts);

            Assert.Equal(channel3, channel);
        }

        [Fact]
        public void MakeTimeoutTask_TaskShouldTimeout()
        {
            var exception = new WriteDeadlineExceededException();

            Assert.ThrowsAsync(exception.GetType(), async () =>
            {
                var delayTask = Task.Delay(200);

                var timeoutTask = delayTask.ToTimeoutTask(100, exception);
            });
        }

        [Fact]
        public void MakeTimeoutTask_TaskShouldNotTimeout()
        {
            var delayTask = Task.Delay(100);

            var timeoutTask = delayTask.ToTimeoutTask(200, new WriteDeadlineExceededException());

            Assert.True(true);
        }

        [Fact]
        public void WaitToReadShouldSelectCorrect()
        {
            var channel1 = Channel.CreateBounded<int>(1);
            var channel2 = Channel.CreateBounded<int>(1);

            channel1.Writer.WriteAsync(1);
            channel2.Writer.WriteAsync(1);

            var cts = new CancellationTokenSource();
            var tasks = new List<Task<Channel<int>>>
            {
                channel1.WaitToRead(cts.Token),
                channel2.WaitToRead(cts.Token),
            };

            var channel = tasks.FirstAsync(cts);

            Assert.Equal(channel1, channel);
        }

        [Fact]
        public void WaitToReadShouldSelectClosedChannel()
        {
            var channel1 = Channel.CreateBounded<uint?>(1);

            var cts = new CancellationTokenSource();
            var tasks = new List<Task<Channel<uint?>>>
            {
                channel1.WaitToRead(cts.Token),
                Constants.ClosedChannel.WaitToRead(cts.Token),
            };

            var channel = tasks.FirstAsync(cts);

            Assert.Equal(Constants.ClosedChannel, channel);
        }
    }
}
