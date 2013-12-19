using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Client
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("Please supply Search server URI");
                return;
            }

            ServerProxy app = new ServerProxy(@"http://" + args[0]);

            app.run();
        }
    }
}
