using Registeration;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;


namespace FlightSearchServer
{
    /// <summary>
    /// Business logic class of Search server.
    /// this class encapsulates all business logic which. 
    /// </summary>
    public sealed class FlightSearchLogic
    {
        // This is ok as long as we call instance BEFORE anything else
        private static readonly FlightSearchLogic instance = new FlightSearchLogic();

        private FlightSearchLogic() { }

        /// <summary>
        /// Singleton getter/setter
        /// </summary>
        public static FlightSearchLogic Instance
        {
            get
            {
                return instance;
            }
        }

        /// <summary>
        /// Associate seller names with their resources
        /// </summary>
        public ConcurrentDictionary<string, > delegates =
           new ConcurrentDictionary<string, >(Environment.ProcessorCount, Environment.ProcessorCount * 2);

        /// <summary>
        /// Ticket seller registration service.
        /// Publishing mechanism to allow sellers dynamically register our ticekts selling server
        /// </summary>
        private ServiceHost tsrHost;
        /// <summary>
        /// Client search request service
        /// Publishing mechanism to allow clients dynamically make search queries to all registered 
        /// sellers
        /// </summary>
        private ServiceHost cqsHost;

        /// <summary>
        /// Boolean variable to indicate if the singleton was initialized
        /// Initialization is sucessful if publishing services started correctly (Didn't throw an exception)
        /// </summary>
        private bool isInitialized = false;

        /// <summary>
        /// This function will initialize the publishing services. search server will not run if 
        /// those services didn't initialize correctly.
        /// </summary>
        /// <param name="clientPort">Port for client publishing server</param>
        /// <param name="sellerPort">Port for sellers registration publishing server</param>
        public void Initialize(string clientPort, string sellerPort)
        {
            tsrHost = new ServiceHost(typeof(AirSellerRegisteration), new Uri(@"http://localhost:" + sellerPort + @"/Services/FlightsSearchReg"));
            cqsHost = new ServiceHost(typeof(ClientQueryService), new Uri(@"http://localhost:" + clientPort + @"/Services/FlightsSearch"));

            isInitialized = true;
        }

        /// <summary>
        /// Run servers and serve requests
        /// </summary>
        public void run()
        {
            if (!isInitialized)
                return;

            Console.WriteLine("Running");
            try
            {
                tsrHost.Open();
                cqsHost.Open();
            }
            catch (Exception)
            {
                if (tsrHost != null)
                    tsrHost.Close();
                if (cqsHost != null)
                    cqsHost.Close();
            }
        }

        /// <summary>
        /// This function is delegated by the Client Query service. 
        /// it will iterate all sellers and make the appropriate client requested 
        /// query, returns search results to the client
        /// </summary>
        /// <param name="src">Source of flight</param>
        /// <param name="dst">Destination of flight</param>
        /// <param name="date">Date of flight</param>
        /// <returns>Flights from all sellers which match the input criterias</returns>
        public QueryResultTrips QueryTrips(string src, string dst, string date, string companies)
        {
            Console.WriteLine("FlightSearchServer: " + dst + " " + src + " " + date);

            try // Sanitize date
            {
                DateTime.ParseExact(date, "dd/MM/yyyy", CultureInfo.InvariantCulture);
            }
            catch (Exception)
            {
                throw new FlightSearchServerBadDate();
            }
            List<string> relevantSellers = companies.Split(',').ToList();


            foreach (? delegateMachine in delegates.Values)
            {
                delegateMachine.getTrips(src,dst,DateTime.Parse(date),relevantSellers);
                //sellers[cluster].GetFlights(
            }
            QueryResultFlights flights = new QueryResultFlights();
            foreach (var seller in sellers.Keys)
            {
                FlightQuery fq = new FlightQuery();
                fq.src = src;
                fq.dst = dst;
                fq.date = DateTime.Parse(date);
                using (new OperationContextScope((IContextChannel)sellers[seller]))
                {
                    try
                    {
                        Flights sellerFlights =
                            sellers[seller].GetFlights(fq); // DEAL WITH EXCEPTIONS HERE

                        foreach (var sellerFlight in sellerFlights)
                        {
                            QueryResultFlight f1 = (QueryResultFlight)sellerFlight;
                            f1.name = seller;
                            flights.Add(f1);
                        }
                    }
                    catch (FaultException e)
                    {
                        Console.WriteLine("Seller {0} failed with {1}, ignoring.", seller, e.Reason.ToString());
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Seller {0} {1} malfunction: \n{2}", seller, "search", e.Message.ToString());
                        ITicketSellingQueryService victim;
                        sellers.TryRemove(seller, out victim);
                    }
                }
            }

            flights.Sort();

            return flights;
        }


    }
}
