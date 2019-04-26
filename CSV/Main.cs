namespace CSV
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using CsvHelper;
    using CsvHelper.Configuration;
    using log4net;
    using Vanilla;

    class Product
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public double Price { get; set; }
    }

    internal class Main
    {
        private static ConcurrentQueue<Product> _products = new ConcurrentQueue<Product>();

        private static readonly ILog Logger = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        internal static async Task Work(string[] args)
        {
            var source = new CancellationTokenSource();
            var token = source.Token;

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
            Task.Run(async () =>
            {
                var conf = new Configuration();

                conf.Encoding = Encoding.UTF8;
                conf.CultureInfo = CultureInfo.InvariantCulture;

                using (var stream = File.OpenWrite("products.txt"))
                using (var streamWriter = new StreamWriter(stream))

                using (var writer = new CsvWriter(streamWriter, conf))
                {
                    writer.WriteHeader<Product>();
                    writer.NextRecord();

                    while (true)
                    {
                        if (token.IsCancellationRequested)
                        {
                            streamWriter.Flush();
                            return;
                        }

                        Product product = null;

                        while (_products.TryDequeue(out product))
                        {
                            writer.WriteRecord(product);
                            writer.NextRecord();
                        }

                        // No data, let's delay
                        await Task.Delay(500);
                    }
                }
            }, token);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

            var task1 = Task.Run(() =>
            {
                foreach (var number in Enumerable.Range(1, 10))
                {
                    var product = new Product
                    {
                        Id = number,
                        Name = "Product " + number,
                        Price = Math.Round((10d * number) / DateTime.Now.Second, 2)
                    };

                    _products.Enqueue(product);

                    Task.Delay(150).Wait();
                }
            });

            var task2 = Task.Run(() =>
            {
                foreach (var number in Enumerable.Range(11, 10))
                {
                    var product = new Product
                    {
                        Id = number,
                        Name = "Product " + number,
                        Price = Math.Round((10d * number) / DateTime.Now.Second, 2)
                    };

                    _products.Enqueue(product);

                    Task.Delay(150).Wait();
                }
            });

            Task.WaitAll(task1, task2);

            source.Cancel();
        }
    }
}
