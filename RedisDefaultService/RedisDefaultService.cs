using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Fabric;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;

namespace RedisDefaultService
{
    /// <summary>
    /// The FabricRuntime creates an instance of this class for each service type instance. 
    /// </summary>
    internal sealed class RedisDefaultService : StatelessService
    {
        private const int RedisServerPort = 6379;

        public RedisDefaultService(StatelessServiceContext context)
            : base(context)
        { }

        /// <summary>
        /// Optional override to create listeners (like tcp, http) for this service instance.
        /// </summary>
        /// <returns>The collection of listeners.</returns>
        protected override IEnumerable<ServiceInstanceListener> CreateServiceInstanceListeners()
        {
            return new ServiceInstanceListener[0];
        }

        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            await ShutdownRedisAsync();
            string workdir = Path.Combine(Context.CodePackageActivationContext.WorkDirectory, "RedisServer");
            await PrepareRedisEnvironmentAsync(workdir);
            StartRedis(workdir);
            // TODO Monitor the instance somehow
            try
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            finally
            {
                await ShutdownRedisAsync();
            }
        }

        private void StartRedis(string workdir)
        {
            string codePath = Context.CodePackageActivationContext.GetCodePackageObject("Code").Path;

            var startInfo = new ProcessStartInfo(Path.Combine(codePath, "redis-server.exe"))
            {
                WorkingDirectory = workdir,
                UseShellExecute = false,
                Arguments = "default.conf",
            };

            Process.Start(startInfo);

            ServiceEventSource.Current.ServiceMessage(Context, "Redis Server process started");
        }

        private async Task PrepareRedisEnvironmentAsync(string workdir)
        {
            Directory.CreateDirectory(workdir);

            string configTemplate = Path.Combine(Context.CodePackageActivationContext.GetConfigurationPackageObject("Config").Path, @"redis\default.conf");
            string configFileName = Path.Combine(workdir, "default.conf");

            using (var configFileStream = File.Open(configFileName, FileMode.Create, FileAccess.Write, FileShare.Delete))
            {
                using (var templateStream = File.Open(configTemplate, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    await templateStream.CopyToAsync(configFileStream);
                }
            }
        }

        private async Task ShutdownRedisAsync()
        {
            try
            {
                using (var client = new TcpClient())
                {
                    await client.ConnectAsync("127.0.0.1", RedisServerPort);
                    using (var stream = client.GetStream())
                    {
                        byte[] message = Encoding.ASCII.GetBytes("shutdown\n");
                        await stream.WriteAsync(message, 0, message.Length);
                        await stream.FlushAsync();
                    }
                }
            }
            catch (Exception ex)
            {
                // It's entirely possible that we don't have anything on the other end
                ServiceEventSource.Current.ServiceMessage(this.Context, "ShutdownRedis: {0} {1}", ex.GetType(), ex.Message);
            }
        }
    }
}
