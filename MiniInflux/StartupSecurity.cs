/// <summary>
/// Fail-closed validation for security-related startup options. These checks live outside Program.cs
/// so they can be covered without starting the web host.
/// </summary>
public static class StartupSecurity
{
    public static void Validate(MiniInfluxOptions options)
    {
        if (options.Auth.Enabled
            && (string.IsNullOrWhiteSpace(options.Auth.Username) || string.IsNullOrWhiteSpace(options.Auth.Password)))
        {
            throw new InvalidOperationException(
                "Auth.Enabled is true but Auth.Username/Auth.Password is missing. Refusing to start with authentication disabled.");
        }

        if (options.Tls.Enabled)
        {
            var certPath = options.Tls.CertPath;
            if (string.IsNullOrWhiteSpace(certPath) || !File.Exists(certPath))
            {
                throw new InvalidOperationException(
                    $"Tls.Enabled is true but Tls.CertPath '{certPath}' does not exist or is unreadable. Refusing to start without TLS.");
            }

            try
            {
                using var stream = File.OpenRead(certPath);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                throw new InvalidOperationException(
                    $"Tls.Enabled is true but Tls.CertPath '{certPath}' is unreadable. Refusing to start without TLS.",
                    ex);
            }
        }

        // Production fail-closed: refuse to start with auth disabled unless the operator has
        // explicitly opted out. Deployments behind a TLS-terminating reverse proxy still need
        // the escape hatch because the proxy terminates TLS but MiniInflux must still authenticate
        // clients hitting the proxy.
        var env = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT")
                  ?? Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
        if (string.Equals(env, "Production", StringComparison.OrdinalIgnoreCase)
            && !options.Auth.Enabled
            && !string.Equals(Environment.GetEnvironmentVariable("MINI_INFLUX_ALLOW_INSECURE_PROD"), "true", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                "Running in Production environment with authentication disabled is refused. " +
                "Set Auth.Enabled=true or, if access is gated by a trusted reverse proxy, " +
                "set MINI_INFLUX_ALLOW_INSECURE_PROD=true to override.");
        }
    }
}
