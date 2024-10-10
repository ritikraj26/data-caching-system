public static class ReadArgs
{
    public static string Dir { get; private set; } = "/tmp";
    public static string DbFilename { get; private set; } = "dump.rdb";
    public static int Port { get; private set; } = 6379;

    public static void ParseCommandLineArguments(string[] args)
    {
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i] == "--dir" && i + 1 < args.Length)
            {
                Dir = args[i + 1];
            }
            else if (args[i] == "--dbfilename" && i + 1 < args.Length)
            {
                DbFilename = args[i + 1];
            }
            else if (args[i] == "--port" && i + 1 < args.Length)
            {
                Port = int.Parse(args[i + 1]);
            }
        }
    }
}