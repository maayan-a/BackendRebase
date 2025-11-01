using System.Diagnostics;

class Program
{
    static void Main(string[] args)
    {
        if (args.Length == 0)
        {
            Console.WriteLine("Usage: dotnet run <input_file_path>");
            return;
        }

        string filePath = args[0];
        if (!File.Exists(filePath))
        {
            Console.WriteLine($"File not found: {filePath}");
            return;
        }

        long totalPrimeCount = 0;
        var stopwatch = Stopwatch.StartNew();

        foreach (var batch in ReadLinesInBatch(filePath, 10000000)) 
        {
            var counts = new Dictionary<long, int>();
            foreach (var n in batch)
            {
                if (counts.ContainsKey(n))
                    counts[n]++;
                else
                    counts[n] = 1;
            }

            Parallel.ForEach(counts.Keys, new ParallelOptions { MaxDegreeOfParallelism = 2 }, n =>
            {
                if (IsPrime(n))
                {
                    Interlocked.Add(ref totalPrimeCount, counts[n]);
                }
            });
        }


        stopwatch.Stop();
        Console.WriteLine($"\nFound {totalPrimeCount} primes in {stopwatch.Elapsed}");
    }

    static IEnumerable<List<long>> ReadLinesInBatch(string filePath, int batchSize)
    {
        List<long> batch = new List<long>(batchSize);

        foreach (var line in File.ReadLines(filePath))
        {
            if (long.TryParse(line, out long n))
                batch.Add(n);

            if (batch.Count == batchSize)
            {
                yield return batch;
                batch = new List<long>(batchSize);
            }
        }

        if (batch.Count > 0)
            yield return batch;
    }

    static bool IsPrime(long n)
    {
        if (n < 2) return false;
        if (n == 2) return true;
        if (n % 2 == 0) return false;

        long limit = (long)Math.Sqrt(n);
        for (long i = 3; i <= limit; i += 2)
            if (n % i == 0)
                return false;

        return true;
    }
}
