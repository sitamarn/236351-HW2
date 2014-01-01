using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Web;

namespace FlightSearchServer
{

    /// <summary>
    /// Collection of Trips which were retrieved by search queries who where delegated
    /// to all the sellers.
    /// This list is propogated to the client as a result
    /// </summary>
    [CollectionDataContract]
    public class QueryResultTrips : List<QueryResultTrip>
    {

        public QueryResultTrips() { }
        public QueryResultTrips(List<QueryResultTrip> trip) : base(trip) { }
        public QueryResultTrips DistinctMe(){
            return new QueryResultTrips(this.Distinct().ToList());
            
        }
    }

    class QueryResultsTripComparer : IEqualityComparer<QueryResultTrip>
    {
        public bool Equals(QueryResultTrip x, QueryResultTrip other)
        {
            if (x.firstFlight.src.Equals(other.firstFlight.src) && x.firstFlight.dst.Equals(other.firstFlight.dst) && x.firstFlight.seller.Equals(other.firstFlight.seller) && x.firstFlight.flightNumber.Equals(other.firstFlight.flightNumber) && x.firstFlight.date.Equals(other.firstFlight.date))
            {
                if (x.secondFlight == null) return (other.secondFlight == null);
                if (other.secondFlight == null) return false;
                return (x.secondFlight.src.Equals(other.secondFlight.src) && x.secondFlight.dst.Equals(other.secondFlight.dst) && x.secondFlight.seller.Equals(other.secondFlight.seller) && x.secondFlight.flightNumber.Equals(other.secondFlight.flightNumber) && x.secondFlight.date.Equals(other.secondFlight.date));
            }
            return false;
        }

        public int GetHashCode(QueryResultTrip obj)
        {
            return 0;
        }
    }

    /// <summary>
    /// A single trip search query result. this class will be contained within the list
    /// QueryResultTrips.
    /// </summary>
    [DataContract]
    public class QueryResultTrip : IComparable, IEquatable<QueryResultTrip>
    {
        [DataMember]
        public int price { get; set; }
        [DataMember]
        public QueryResultFlight firstFlight { get; set; }
        [DataMember]
        public QueryResultFlight secondFlight { get; set; }

        /// <summary>
        /// This function is used to sort the list by spec requirement.
        /// Client is dumb so sorting is done on server
        /// </summary>
        /// <param name="obj">Right hand object to test</param>
        /// <returns>-1 if this is smaller than obj, otherwise 1</returns>
        public int CompareTo(object obj)
        {
            QueryResultTrip otherTrip = (QueryResultTrip)obj;

            if (price < otherTrip.price) { return -1; }
            else { return 1; }
        }

        /*
                public static explicit operator QueryResultFlight(TicketSellingServer.Flight sellerFlight)
                {
                    QueryResultFlight clientFlight = new QueryResultFlight();
                    clientFlight.dst = sellerFlight.dst;
                    clientFlight.src = sellerFlight.src;
                    clientFlight.seats = sellerFlight.seats;
                    clientFlight.price = sellerFlight.price;
                    clientFlight.name = "UNKNOWN_SELLER_WITH_A_REALLY_REALLY_LONG_NAME_WHICH_NOBODY_CARES_ABOUT";
                    clientFlight.flightNumber = sellerFlight.flightNumber;
                    clientFlight.date = sellerFlight.date;

                    return clientFlight;
                }
         * */

        public bool Equals(QueryResultTrip other)
        {
            if (firstFlight.src.Equals(other.firstFlight.src) && firstFlight.dst.Equals(other.firstFlight.dst) && firstFlight.seller.Equals(other.firstFlight.seller) && firstFlight.flightNumber.Equals(other.firstFlight.flightNumber) && firstFlight.date.Equals(other.firstFlight.date))
            {
                if (secondFlight == null) return (other.secondFlight == null);
                if (other.secondFlight == null) return false;
                return (secondFlight.src.Equals(other.secondFlight.src) && secondFlight.dst.Equals(other.secondFlight.dst) && secondFlight.seller.Equals(other.secondFlight.seller) && secondFlight.flightNumber.Equals(other.secondFlight.flightNumber) && secondFlight.date.Equals(other.secondFlight.date));
            }
            return false;
        }
    }


    /// <summary>
    /// A single Flight search query result. this class will be contained within the list
    /// QueryResultFlights.
    /// </summary>
    [DataContract]
    public class QueryResultFlight
    {
        [DataMember]
        public string seller { get; set; }
        [DataMember]
        public string flightNumber { get; set; }
        [DataMember]
        public string src { get; set; }
        [DataMember]
        public string dst { get; set; }
        [DataMember]
        public DateTime date { get; set; }
    }

    /*
        public static explicit operator QueryResultFlight(TicketSellingServer.Flight sellerFlight)
        {
            QueryResultFlight clientFlight = new QueryResultFlight();
            clientFlight.dst = sellerFlight.dst;
            clientFlight.src = sellerFlight.src;
            clientFlight.seats = sellerFlight.seats;
            clientFlight.price = sellerFlight.price;
            clientFlight.name = "UNKNOWN_SELLER_WITH_A_REALLY_REALLY_LONG_NAME_WHICH_NOBODY_CARES_ABOUT";
            clientFlight.flightNumber = sellerFlight.flightNumber;
            clientFlight.date = sellerFlight.date;

            return clientFlight;
        }
     * */
}

