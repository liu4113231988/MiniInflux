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
    }
}
