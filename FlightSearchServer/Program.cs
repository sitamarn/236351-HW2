using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlightSearchServer
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 3)
            {
                Console.WriteLine("Usage: ./server <clients port> <sellers port> <logfile>");
                return;
            }

            try
            {
                Convert.ToInt32(args[0]);
                Convert.ToInt32(args[1]);
            }
            catch (Exception e)
            {
                Console.WriteLine("Invalid client or seller port: {0}", e.Message.ToString());
                return;
            }

            try
            {
                FlightSearchLogic fss = FlightSearchLogic.Instance; // DO NOT REMOVE THIS (THREAD CORRECTNESS)
                fss.Initialize(args[0], args[1], args[2]); // Host services
                fss.run(); // wait till death

                Console.ReadKey();
            }
            catch (Exception e)
            {
                Console.WriteLine("Program failed to launch because:" + e.Message);
            }
        }
    }
}
