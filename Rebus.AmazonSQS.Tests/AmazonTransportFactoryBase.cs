using System;
using System.IO;
using System.Collections.Concurrent;
using System.Collections.Generic;

using Rebus.Transport;
using Rebus.Extensions;
using Rebus.Exceptions;
using Rebus.Tests.Contracts.Transports;

namespace Rebus.AmazonSQS.Tests
{
    public abstract class AmazonTransportFactoryBase<TTransportOptions> : ITransportFactory where TTransportOptions : class, new()
    {
        private readonly ConcurrentStack<IDisposable> _disposables = new ConcurrentStack<IDisposable>();
        private readonly Dictionary<string, ITransport> _queuesToDelete = new Dictionary<string, ITransport>();
        private readonly Action<ITransport> fnDeleteTransport;

        protected AmazonTransportFactoryBase(Action<ITransport> fnDeleteTransport)
        {
            this.fnDeleteTransport = fnDeleteTransport;
        }

        public ITransport Create(string inputQueueAddress, TimeSpan peeklockDuration, TTransportOptions options = null)
        {
            if (inputQueueAddress == null)
            {
                // one-way client
                var transport = CreateInstance(null, peeklockDuration, options);
                if (transport is IDisposable disposable)
                {
                    _disposables.Push(disposable);
                }

                return transport;
            }

            return _queuesToDelete.GetOrAdd(inputQueueAddress, () =>
            {
                var transport = CreateInstance(inputQueueAddress, peeklockDuration, options);
                if (transport is IDisposable disposable)
                {
                    _disposables.Push(disposable);
                }

                return transport;
            });
        }

        public void CleanUp()
        {
            CleanUp(false);
        }

        public void CleanUp(bool deleteQueues)
        {
            if (deleteQueues)
            {
                foreach (var queueAndTransport in _queuesToDelete)
                {
                    var transport = queueAndTransport.Value;
                    this.fnDeleteTransport(transport);
                }
            }

            while (_disposables.TryPop(out var disposable))
            {
                Console.WriteLine($"Disposing {disposable}");
                disposable.Dispose();
            }
        }

        public ITransport CreateOneWayClient()
        {
            return Create(null, TimeSpan.FromSeconds(30));
        }

        public ITransport Create(string inputQueueAddress)
        {
            return Create(inputQueueAddress, TimeSpan.FromSeconds(30));
        }

        protected abstract ITransport CreateInstance(string inputQueueAddress, TimeSpan peeklockDuration, TTransportOptions options);

        protected static string GetFilePath(string fileName)
        {
            var baseDirectory = AppContext.BaseDirectory;

            // added because of test run issues on MacOS
            var indexOfBin = baseDirectory.LastIndexOf("bin", StringComparison.OrdinalIgnoreCase);
            var connectionStringFileDirectory = baseDirectory.Substring(0, (indexOfBin > 0) ? indexOfBin : baseDirectory.Length);
            return Path.Combine(connectionStringFileDirectory, fileName);
        }

        protected static ConnectionInfo ConnectionInfoFromEnvironmentVariable(string environmentVariableName)
        {
            var value = Environment.GetEnvironmentVariable(environmentVariableName);

            if (value == null)
            {
                Console.WriteLine("Could not find env variable {0}", environmentVariableName);
                return null;
            }

            Console.WriteLine("Using Amazon connection info from env variable {0}", environmentVariableName);
            return ConnectionInfo.CreateFromString(value);
        }

        protected static ConnectionInfo ConnectionInfoFromFileOrNull(string filePath)
        {
            if (!File.Exists(filePath))
            {
                Console.WriteLine("Could not find file {0}", filePath);
                return null;
            }

            Console.WriteLine("Using Amazon connection info from file {0}", filePath);
            return ConnectionInfo.CreateFromString(File.ReadAllText(filePath));
        }

        protected static ConnectionInfo Throw(string message)
        {
            throw new RebusConfigurationException(message);
        }
    }
}
