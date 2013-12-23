using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;

namespace AirlineServer
{
    class Program
    {
        static void Main(string[] args)
        {
            string clientPort = args[3];
            ServiceHost cqsHost = new ServiceHost(typeof(ISellerClusterService), new Uri(@"http://localhost:" + clientPort + @"/Services/FlightsSearch"));


        }
    }
}
