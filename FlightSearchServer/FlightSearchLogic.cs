﻿using AirlineServer;
using Registeration;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.ServiceModel;
using System.ServiceModel.Description;
using System.ServiceModel.Web;
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
        public ConcurrentDictionary<string, ISellerService> delegates =
           new ConcurrentDictionary<string, ISellerService>(Environment.ProcessorCount, Environment.ProcessorCount * 2);

        /// <summary>
        /// Ticket seller registration service.
        /// Publishing mechanism to allow sellers dynamically register our ticekts selling server
        /// </summary>
        private WebServiceHost tsrHost;
        /// <summary>
        /// Client search request service
        /// Publishing mechanism to allow clients dynamically make search queries to all registered 
        /// sellers
        /// </summary>
        private WebServiceHost cqsHost;

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
        public void Initialize(string clientPort, string sellerPort, string logFileName)
        {
            tsrHost = new WebServiceHost(typeof(AirSellerRegisteration), new Uri(@"http://localhost:" + sellerPort + @"/Services/FlightsSearchReg"));
            cqsHost = new WebServiceHost(typeof(ClientQueryService), new Uri(@"http://localhost:" + clientPort + @"/Services"));
           // ServiceEndpoint regEndPoint = tsrHost.AddServiceEndpoint(typeof(IAirSellerRegisteration), new WebHttpBinding(), @"http://localhost:" + sellerPort + @"/Services/FlightsSearchReg");
            ServiceEndpoint sellerEndPoint = cqsHost.AddServiceEndpoint(typeof(IClientQueryService), new WebHttpBinding(), @"http://localhost:" + clientPort + @"/Services/FlightsSearch");

            ExOpBehavior logBehavior = new ExOpBehavior(logFileName);
            foreach (OperationDescription description in sellerEndPoint.Contract.Operations)
            {
                if (description.Name.Equals("GetFlights"))
                    description.Behaviors.Add(logBehavior);
            }
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

        public void dispose()
        {
            try
            {
                if (tsrHost != null)
                    tsrHost.Close();
                if (cqsHost != null)
                    cqsHost.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine("Cant dispose services: " + e.Message);
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
            string[] companesArr = companies.Split(',');
            if(companies.Equals("")) companesArr = new String[0];
            Console.WriteLine("FlightSearchServer: " + dst + " " + src + " " + date);
            DateTime dateOfFlight;
            try // Sanitize date
            {
                dateOfFlight = DateTime.ParseExact(date, "dd/MM/yyyy", CultureInfo.InvariantCulture);
            }
            catch (Exception)
            {
                throw new FlightSearchServerBadDate();
            }

            QueryResultTrips queryTrips = new QueryResultTrips();

            if (delegates.Count == 0)
            {
                string errMsg = ("no delegate is available");
                errMsg += ("the system waits for a new delegate registeration");
                errMsg += ("try again later...");
                WebOperationContext.Current.OutgoingResponse.SetStatusAsNotFound(errMsg);
                return null;
            }

            foreach (ISellerService delegateMachine in delegates.Values)
            {
                Trip[] trips = null;
                try
                {
                    trips = delegateMachine.getTrips(src, dst, dateOfFlight, companesArr);
                }

                catch (FaultException e)
                {
                    Console.WriteLine("a delegate machine is not available: "+e.Message);
                }
                foreach (Trip trip in trips)
                {
                    
                    QueryResultTrip queryTrip = new QueryResultTrip();
                    queryTrip.firstFlight = new QueryResultFlight();
                    queryTrip.firstFlight.src = trip.firstFlight.src;
                    queryTrip.firstFlight.dst = trip.firstFlight.dst;
                    queryTrip.firstFlight.date = trip.firstFlight.date;
                    queryTrip.firstFlight.seller = trip.firstFlight.seller;
                    queryTrip.firstFlight.flightNumber = trip.firstFlight.flightNumber;
                    if (trip.secondFlight != null)
                    {
                        queryTrip.secondFlight = new QueryResultFlight();
                        queryTrip.secondFlight.src = trip.secondFlight.src;
                        queryTrip.secondFlight.dst = trip.secondFlight.dst;
                        queryTrip.secondFlight.date = trip.secondFlight.date;
                        queryTrip.secondFlight.flightNumber = trip.secondFlight.flightNumber;
                        queryTrip.secondFlight.seller = trip.secondFlight.seller;
                    }
                    else
                    {
                        queryTrip.secondFlight = null;
                    }
                    
                    queryTrip.price = trip.price;

                    queryTrips.Add(queryTrip);
                }
            }
            // remove duplicates
            queryTrips = new QueryResultTrips(queryTrips.ToList().Distinct(new QueryResultsTripComparer()).ToList());
            // sort as requested
            queryTrips.Sort();

            return queryTrips;
        }


    }
}
