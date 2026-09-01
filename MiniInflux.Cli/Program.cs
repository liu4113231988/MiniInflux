using Microsoft.Extensions.Configuration;
using MiniInflux.Net10.Storage;

var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: true)
    .AddEnvironmentVariables()
    .Build();

var options = MiniInfluxOptions.Load(configuration);
if (args.Length == 0 || args[0] is "help" or "--help" or "-h")
{
    var helpExitCode = ManagementCli.TryRun(args.Length == 0 ? ["help"] : args, options, Console.Out, Console.Error);
    return helpExitCode ?? 1;
}

try
{
    using var dataDirectoryLock = DataDirectoryLock.Acquire(options.DataPath);
    var exitCode = ManagementCli.TryRun(args, options, Console.Out, Console.Error);
    if (!exitCode.HasValue)
    {
        Console.Error.WriteLine($"unsupported command: {args[0]}");
        return 1;
    }

    return exitCode.Value;
}
catch (IOException ex)
{
    Console.Error.WriteLine(ex.Message);
    return 2;
}
