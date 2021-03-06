﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Fabric;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.ServiceFabric.Services.Communication.AspNetCore;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;

namespace RedisSentinelService
{
    /// <summary>
    /// The FabricRuntime creates an instance of this class for each service type instance. 
    /// </summary>
    internal sealed class RedisSentinelService : StatelessService
    {
        private const int SentinelPort = 26379;
        private const int RedisServerPort = 6379;

        public RedisSentinelService(StatelessServiceContext context)
            : base(context)
        { }

        /// <summary>
        /// Optional override to create listeners (like tcp, http) for this service instance.
        /// </summary>
        /// <returns>The collection of listeners.</returns>
        protected override IEnumerable<ServiceInstanceListener> CreateServiceInstanceListeners()
        {
            return new ServiceInstanceListener[]
            {
                new ServiceInstanceListener(serviceContext =>
                    new KestrelCommunicationListener(serviceContext, "ServiceEndpoint", (url, listener) =>
                    {
                        ServiceEventSource.Current.ServiceMessage(serviceContext, $"Starting Kestrel on {url}");

                        return new WebHostBuilder()
                                    .UseKestrel()
                                    .ConfigureServices(
                                        services => services
                                            .AddSingleton<StatelessServiceContext>(serviceContext))
                                    .UseContentRoot(Directory.GetCurrentDirectory())
                                    .UseStartup<Startup>()
                                    .UseServiceFabricIntegration(listener, ServiceFabricIntegrationOptions.UseReverseProxyIntegration)
                                    .UseUrls(url)
                                    .Build();
                    }))
            };
        }

        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            await ShutdownSentinelAsync();
            var nodes = await GetListOfClusterNodesAsync();
            string workdir = Path.Combine(Context.CodePackageActivationContext.WorkDirectory, "RedisSentinel");
            await PrepareSentinelEnvironmentAsync(workdir, nodes);
            StartSentinel(workdir);
            // TODO Monitor the instance somehow
            try
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            finally
            {
                await ShutdownSentinelAsync();
            }
        }

        private void StartSentinel(string workdir)
        {
            string codePath = Context.CodePackageActivationContext.GetCodePackageObject("Code").Path;

            var startInfo = new ProcessStartInfo(Path.Combine(codePath, "redis-server.exe"))
            {
                WorkingDirectory = workdir,
                UseShellExecute = false,
                Arguments = "sentinel.conf --sentinel",
            };

            Process.Start(startInfo);
            
            ServiceEventSource.Current.ServiceMessage(Context, "Redis Sentinel process started");
        }

        private async Task PrepareSentinelEnvironmentAsync(string workdir, List<ClusterNodeInfo> nodes)
        {
            Directory.CreateDirectory(workdir);

            string configTemplate = Path.Combine(Context.CodePackageActivationContext.GetConfigurationPackageObject("Config").Path, @"sentinel\sentinel.conf");
            string configFileName = Path.Combine(workdir, "sentinel.conf");

            int quorum = (nodes.Count <= 2) ? 1 : 2;

            using (var configFileStream = File.Open(configFileName, FileMode.Create, FileAccess.Write, FileShare.Delete))
            {
                using (var templateStream = File.Open(configTemplate, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    await templateStream.CopyToAsync(configFileStream);
                }
                using (var writer = new StreamWriter(configFileStream, Encoding.ASCII))
                {
                    await writer.WriteLineAsync("# Configuration added by RedisSentinelService");
                    await writer.WriteLineAsync($"sentinel monitor default {nodes[0].Address} {RedisServerPort} {quorum}");
                    await writer.WriteLineAsync("# Generated by CONFIG REWRITE");
                    await writer.WriteLineAsync("sentinel config-epoch default 0");
                    await writer.WriteLineAsync("sentinel leader-epoch default 1");
                    foreach (var node in nodes.Skip(1))
                    {
                        await writer.WriteLineAsync($"sentinel known-slave default {node.Address} {RedisServerPort}");
                    }
                    await writer.WriteLineAsync("sentinel current-epoch 1");
                }
            }
        }

        private async Task<List<ClusterNodeInfo>> GetListOfClusterNodesAsync()
        {
            var result = new List<ClusterNodeInfo>();
            using (var fabricClient = new FabricClient())
            {
                var nodeList = await fabricClient.QueryManager.GetNodeListAsync(null);
                while (true)
                {
                    foreach (var node in nodeList)
                    {
                        result.Add(new ClusterNodeInfo
                        {
                            NodeName = node.NodeName,
                            Address = node.IpAddressOrFQDN,
                        });
                    }
                    if (string.IsNullOrEmpty(nodeList.ContinuationToken))
                        break;
                    nodeList = await fabricClient.QueryManager.GetNodeListAsync(null, nodeList.ContinuationToken);
                }
            }
            return result;
        }

        private async Task ShutdownSentinelAsync()
        {
            try
            {
                using (var client = new TcpClient())
                {
                    await client.ConnectAsync("127.0.0.1", SentinelPort);
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
                ServiceEventSource.Current.ServiceMessage(this.Context, "ShutdownSentinel: {0} {1}", ex.GetType(), ex.Message);
            }
        }
    }
}
