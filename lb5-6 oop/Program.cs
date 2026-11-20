using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;

namespace PraktMultithreading
{
    internal static class Program
    {
        private static void Main()
        {
            Console.OutputEncoding = Encoding.UTF8;
            Task1_Threads();
            Task2_RaceAndLock();
            Task3_DeadlockSemaphoreMutex();
            Task4_ProducerConsumer();
            Task5_Performance();
        }

        // ЗАВДАННЯ 1: ThreadStart, ParameterizedThreadStart, Join, IsBackground, замикання
        private static void Task1_Threads()
        {
            Console.WriteLine("=== ЗАВДАННЯ 1: Потоки, Join, IsBackground та замикання ===");
            var t1 = new Thread(() => {
                for (int k = 1; k <= 2; k++) { Console.WriteLine($"[{Thread.CurrentThread.Name}] крок {k}"); Thread.Sleep(15); }
            })
            { Name = "T-Start" };
            var t2 = new Thread(obj => {
                for (int k = 1; k <= (int)obj!; k++) { Console.WriteLine($"[{Thread.CurrentThread.Name}] крок {k}"); Thread.Sleep(15); }
            })
            { Name = "T-Param" };

            t1.Start(); t2.Start(2);
            t1.Join(); t2.Join();

            var bg = new Thread(() => { Thread.Sleep(200); Console.WriteLine("[BG] не встигне якщо Main завершиться"); }) { IsBackground = true };
            bg.Start();
            Console.WriteLine($"Фоновий потік IsBackground={bg.IsBackground} запущено (при виході з Main CLR перерве його).");

            var bad = new List<int>(); var good = new List<int>();
            var threads = new Thread[6];
            for (int i = 0; i < 3; i++)
            {
                threads[i] = new Thread(() => { lock (bad) bad.Add(i); }); // Помилка замикання
                int copy = i;
                threads[i + 3] = new Thread(() => { lock (good) good.Add(copy); }); // Виправлено
            }
            foreach (var t in threads) t.Start();
            foreach (var t in threads) t.Join();
            Console.WriteLine($"Замикання (помилка): [{string.Join(", ", bad)}] | Виправлено: [{string.Join(", ", good)}]\n");
        }

        // ЗАВДАННЯ 2: Гонка даних та lock
        private static void Task2_RaceAndLock()
        {
            Console.WriteLine("=== ЗАВДАННЯ 2: Гонка даних та lock ===");
            const int threadsCount = 4, iters = 250_000;
            int unsafeCounter = 0, safeCounter = 0;
            object sync = new object();

            long RunTest(Action work)
            {
                var sw = Stopwatch.StartNew();
                var arr = new Thread[threadsCount];
                for (int i = 0; i < threadsCount; i++) { arr[i] = new Thread(() => { for (int j = 0; j < iters; j++) work(); }); arr[i].Start(); }
                foreach (var t in arr) t.Join();
                return sw.ElapsedMilliseconds;
            }

            long tUnsafe = RunTest(() => unsafeCounter++);
            long tSafe = RunTest(() => { lock (sync) safeCounter++; });
            Console.WriteLine($"Очікувано: {threadsCount * iters:N0}");
            Console.WriteLine($"Без lock:  {unsafeCounter:N0} (час: {tUnsafe} мс)");
            Console.WriteLine($"З lock:    {safeCounter:N0} (час: {tSafe} мс)\n");
        }

        // ЗАВДАННЯ 3: Deadlock, узгоджений порядок, SemaphoreSlim, Mutex
        private static void Task3_DeadlockSemaphoreMutex()
        {
            Console.WriteLine("=== ЗАВДАННЯ 3: Deadlock, SemaphoreSlim та Mutex ===");
            object r1 = new object(), r2 = new object();

            // 1. Відтворення deadlock та виявлення по таймауту
            var d1 = new Thread(() => { lock (r1) { Thread.Sleep(40); if (Monitor.TryEnter(r2, 150)) Monitor.Exit(r2); } });
            var d2 = new Thread(() => { lock (r2) { Thread.Sleep(40); if (Monitor.TryEnter(r1, 150)) Monitor.Exit(r1); } });
            d1.Start(); d2.Start();
            Console.WriteLine($"Deadlock виявлено (таймаут очікування): {!d1.Join(120)}");
            d1.Join(); d2.Join();

            // 2. Усунення: однаковий порядок r1 -> r2
            var ok1 = new Thread(() => { lock (r1) { lock (r2) { } } });
            var ok2 = new Thread(() => { lock (r1) { lock (r2) { } } });
            ok1.Start(); ok2.Start(); ok1.Join(); ok2.Join();
            Console.WriteLine("Узгоджений порядок (r1 -> r2): обидва потоки завершено штатно.");

            // 3. SemaphoreSlim (2 слоти на 4 потоки) + іменований Mutex
            using var sem = new SemaphoreSlim(2, 2);
            using var mutex = new Mutex(false, "Global\\Prakt10_DemoMutex");
            var workers = new Thread[4];
            for (int i = 0; i < 4; i++)
            {
                int id = i + 1;
                workers[i] = new Thread(() => {
                    Console.WriteLine($"[Client-{id}] очікує слот (вільно: {sem.CurrentCount})");
                    sem.Wait();
                    Console.WriteLine($"[Client-{id}] -> отримав доступ (вільно: {sem.CurrentCount})");
                    Thread.Sleep(50);
                    sem.Release();
                });
                workers[i].Start();
            }
            foreach (var w in workers) w.Join();
            Console.WriteLine($"Іменований Mutex створено успішно (SafeWaitHandle.IsInvalid={mutex.SafeWaitHandle.IsInvalid}).\n");
        }

        // ЗАВДАННЯ 4: Виробник-Споживач (Monitor.Wait / PulseAll)
        private static void Task4_ProducerConsumer()
        {
            Console.WriteLine("=== ЗАВДАННЯ 4: Виробник-Споживач (Monitor) ===");
            var buffer = new Queue<int>();
            const int capacity = 3, totalItems = 10;
            object locker = new object();
            int consumed = 0;

            var producer = new Thread(() => {
                for (int i = 1; i <= totalItems; i++)
                {
                    lock (locker)
                    {
                        while (buffer.Count >= capacity) { Console.WriteLine("[Виробник] буфер повний -> Wait"); Monitor.Wait(locker); }
                        buffer.Enqueue(i);
                        Console.WriteLine($"[Виробник] додав {i} (у буфері: {buffer.Count})");
                        Monitor.PulseAll(locker);
                    }
                    Thread.Sleep(10);
                }
            });

            var consumer = new Thread(() => {
                while (true)
                {
                    lock (locker)
                    {
                        if (consumed >= totalItems) break;
                        while (buffer.Count == 0) { Console.WriteLine("[Споживач] буфер порожній -> Wait"); Monitor.Wait(locker); }
                        int item = buffer.Dequeue();
                        consumed++;
                        Console.WriteLine($"[Споживач] забрав {item} (всього: {consumed}/{totalItems})");
                        Monitor.PulseAll(locker);
                    }
                    Thread.Sleep(25);
                }
            });

            producer.Start(); consumer.Start();
            producer.Join(); consumer.Join();
            Console.WriteLine($"Підсумок: вироблено={totalItems}, спожито={consumed}, залишок у буфері={buffer.Count}\n");
        }

        // ЗАВДАННЯ 5: Продуктивність (1 vs 2 vs N логічних процесорів)
        private static void Task5_Performance()
        {
            Console.WriteLine("=== ЗАВДАННЯ 5: Продуктивність (1 vs 2 vs N потоків) ===");
            const int N = 40_000_000;
            int logicalCpus = Environment.ProcessorCount;

            static double ComputeRange(int start, int end)
            {
                double sum = 0;
                for (int i = start; i <= end; i++) sum += Math.Sqrt(i) * Math.Sin(i * 0.001);
                return sum;
            }

            (double res, long ms) MeasureParallel(int threads)
            {
                var sw = Stopwatch.StartNew();
                double[] partial = new double[threads];
                var arr = new Thread[threads];
                int chunk = N / threads;
                for (int t = 0; t < threads; t++)
                {
                    int idx = t, s = idx * chunk + 1, e = (idx == threads - 1) ? N : (idx + 1) * chunk;
                    arr[t] = new Thread(() => partial[idx] = ComputeRange(s, e));
                    arr[t].Start();
                }
                foreach (var t in arr) t.Join();
                double total = 0;
                foreach (double p in partial) total += p;
                return (total, sw.ElapsedMilliseconds);
            }

            var r1 = MeasureParallel(1);
            var r2 = MeasureParallel(2);
            var rN = MeasureParallel(logicalCpus);

            Console.WriteLine($"Логічних процесорів ОС: {logicalCpus}");
            Console.WriteLine($"1 потік:        {r1.ms} мс | сума = {r1.res:F2}");
            Console.WriteLine($"2 потоки:       {r2.ms} мс | прискорення = {(double)r1.ms / Math.Max(1, r2.ms):F2}x");
            Console.WriteLine($"{logicalCpus} потоків (CPU): {rN.ms} мс | прискорення = {(double)r1.ms / Math.Max(1, rN.ms):F2}x");
        }
    }
}