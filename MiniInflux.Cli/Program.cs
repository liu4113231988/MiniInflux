using MiniInflux.Net10.Storage;

namespace MiniInflux.Cli;

/// <summary>
/// Entry point for the <c>miniinfluxctl</c> management CLI. The class lives in a namespace (rather
/// than relying on top-level statements) so the generated <c>Program</c> type does not collide with
/// the web application's <c>Program</c> type when both assemblies are referenced together.
/// </summary>
internal static class CliEntryPoint
{
    public static int Main(string[] args)
    {
        if (args.Length == 0 || args[0] is "help" or "--help" or "-h")
        {
            var options = new MiniInfluxOptions();
            var helpExitCode = ManagementCli.TryRun(args.Length == 0 ? ["help"] : args, options, Console.Out, Console.Error);
            return helpExitCode ?? 1;
        }

        try
        {
            var options = CommandLineOptions.Parse(args);
            using var dataDirectoryLock = DataDirectoryLock.Acquire(options.DataPath);
            var exitCode = ManagementCli.TryRun(args, options, Console.Out, Console.Error);
            if (!exitCode.HasValue)
            {
                Console.Error.WriteLine($"unsupported command: {args[0]}");
                return 1;
            }

            return exitCode.Value;
        }
        catch (Exception ex) when (ex is IOException or ArgumentException)
        {
            Console.Error.WriteLine(ex.Message);
            return 2;
        }
    }
}

