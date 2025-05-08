using MediatR;
using Microsoft.Extensions.Logging;
using PLCStandardInterfaces.Helpers;
using PLCStandardInterfaces.Models.DataFeed.MasterDataResponse;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using D4G.DataAccess;
using System.Data;
using Dapper;
using PLCStandardInterfaces.Models.DataFeed.MasterDataTableResponses;
using DocumentFormat.OpenXml.Office2010.Excel;
namespace PLCStandardInterfaces.Database.Handlers.DataFeed.MasterData
{
    public class GetSupplierDataQuery : IRequest<SupplierResponse>
    {
        public DateTime? DateTimeFromUTC { get; set; }
        public int PageNumber { get; set; }
        public int PageSize { get; set; }

        public GetSupplierDataQuery(DateTime? dateTimeFromUTC, int pageNumber, int pageSize)
        {
            DateTimeFromUTC = dateTimeFromUTC;
            PageNumber = pageNumber;
            PageSize = pageSize;
        }
    }
    public class GetSupplierDataHandler : IRequestHandler<GetSupplierDataQuery, SupplierResponse>
    {
        private readonly DataContext _context;
        private readonly ILogger<GetSupplierDataHandler> _logger;
        private readonly IDapperContext _dapperContext;
        private readonly IConnectionManager _connection;

        public GetSupplierDataHandler(DataContext context, ILogger<GetSupplierDataHandler> logger, IDapperContext dapperContext, IConnectionManager connection)
        {
            _context = context;
            _logger = logger;
            _dapperContext = dapperContext;
            _connection = connection;
        }

        public async Task<SupplierResponse> Handle(GetSupplierDataQuery request, CancellationToken cancellationToken)
        {
            try
            {
                var args = new DynamicParameters();
                args.Add("@DateTimeFromUTC", request.DateTimeFromUTC == null ? null : request.DateTimeFromUTC.Value);
                args.Add("@PageSize", request.PageSize);
                args.Add("@PageNumber", request.PageNumber);

                var reader = await _dapperContext.Connection.QueryMultipleAsync("DataFeed.uspGetSupplierIds", args, commandType: CommandType.StoredProcedure);
                var SupplierStatus = await reader.ReadAsync<SupplierStatus>();
                var totalRecords = await reader.ReadSingleOrDefaultAsync<int>();
                

                var idTable = new System.Data.DataTable();
                idTable.Columns.Add("ID", typeof(long));
                foreach (var d in SupplierStatus)
                {
                    idTable.Rows.Add(d.Id);
                }

                var parameters = new DynamicParameters();
                parameters.Add("@Ids", idTable.AsTableValuedParameter("dbo.udtIDs"));

                List<SupplierTable> supplierData = new List<SupplierTable>();

                supplierData = await ConnectionManager
                     .StoredProc<SupplierTable>(_connection.Connection, "DataFeed.uspGetSuppliersBySupplierIds", parameters)
                     .Include<SupplierTable, SupplierAddress>((c, a) => c.Addresses = a.Where(a => a.SupplierId == c.Id).ToList())
                     .Include<SupplierTable, SupplierContact>((c, a) => c.Contacts = a.Where(a => a.SupplierId == c.Id).ToList())
                     .Include<SupplierTable, SupplierProfileDetails>((c, a) => c.Profiles = a.Where(a => a.SupplierId == c.Id).ToList())
                     .Include<SupplierTable, SupplierClientTable>((c, a) => c.Clients = a.Where(a => a.SupplierId == c.Id).ToList())
                     .Include<SupplierTable, SupplierProductTable>((c, a) => c.Products = a.Where(a => a.SupplierId == c.Id).ToList())
                     .Include<SupplierTable, SupplierArticleTable>((c, a) => c.Articles = a.Where(a => a.SupplierId == c.Id).ToList())
                     .Include<SupplierTable, SupplierLoadingPointTable>((c, a) => c.LoadingPoints = a.Where(a => a.SupplierId == c.Id).ToList())
                     .Include<SupplierTable, SupplierLoadingPointProductArticleTable>((c, a) => c.ProductArticles = a.Where(a => a.SupplierId == c.Id).ToList())
                     .Include<SupplierTable, SupplierLoadingNumberInfoTable>((c, a) => c.LoadingNumberInfo = a.Where(a => a.SupplierId == c.Id).ToList())
                     .ExecuteAsync();

                var response = supplierData.Select(S =>
                {
                    return new SupplierData
                    {
                        Name = S.Name,
                        Number = S.Number,
                        ERPNumber = S.ERPNumber,
                        ShortName = S.ShortName,   
                        Active  = S.Active,
                        Description = S.Description,
                        Status = SupplierStatus.Where(s => s.Id == S.Id).FirstOrDefault().Status,
                        Addresses = S.Addresses.Select(a =>
                        {
                            return new Address
                            {
                                Street = a.Street,
                                City = a.City,
                                StreetNumber = a.StreetNumber,
                                State = a.State,
                                ZIP = a.ZIP,
                                Country = a.Country,
                                District = a.District,
                                IsDefault = a.IsDefault,
                                Language = a.Language,
                                Latitude = a.Latitude,
                                Longitude = a.Longitude,
                                TimeZone = a.TimeZone
                            };

                        }).ToList(),
                        Contacts = S.Contacts.Select(c =>
                        {
                            return new Contact
                            {
                                FirstName = c.FirstName,
                                LastName = c.LastName,
                                MiddleName = c.MiddleName,
                                Phone = c.Phone,
                                PhoneType = c.PhoneType,
                                Email = c.Email,
                                EmailType = c.EmailType,
                                MatchCode = c.MatchCode
                            };
                        }).ToList(),             
                        Profiles = S.Profiles.Select(P=>
                        {
                            return new SupplierProfile
                            {
                                Name = P.Name,
                                Clients = S.Clients
                                .Where(c => c.SupplierToProfileId == P.SupplierToProfileId)
                                .Select(c => new SupplierClient
                                {
                                    Name = c.Name,
                                    Number = c.Number
                                }).ToList(),
                                Products = S.Products
                                .Where(Po => Po.SupplierToProfileId == P.SupplierToProfileId)
                                .Select(sp => new SupplierProduct
                                {
                                    Product = sp.Product,
                                    ProductNumber = sp.ProductNumber,
                                    articles = S.Articles
                                    .Where(a => a.SupplierToProfileId == P.SupplierToProfileId && a.ProductId == sp.ProductId)
                                    .Select(sa => new SupplierArticle
                                    {
                                        Article = sa.Article,
                                        ArticleNumber = sa.ArticleNumber,
                                        IsAssignedToProduct = sa.IsAssignedToProduct
                                    }).ToList()
                                }).ToList(),
                                LoadingPoints = S.LoadingPoints
                               .Where(lp => lp.SupplierToProfileId == P.SupplierToProfileId)
                               .Select(l => new SupplierLoadingPoint
                               {
                                   Name = l.Name,
                                   Number = l.Number,
                                   products = S.ProductArticles
                                   .Where(pa => pa.SupplierToProfileId == P.SupplierToProfileId && pa.SupplierToLoadingPointId == l.SupplierToLoadingPointId)
                                   .Select(pa => new SupplierLoadingPointProductArticle
                                   {
                                       Product = pa.Product,
                                       Article = pa.Article
                                   }).ToList()
                               }).ToList(),
                                LoadingNumbers = S.LoadingNumberInfo
                               .Where(l => l.SupplierToProfileId == P.SupplierToProfileId)
                               .GroupBy(l => l.ClientName)
                               .Select(g=> g.First())
                               .Select(l => new SupplierLoadingNumbers
                               {
                                   ClientName = l.ClientName,
                                   LoadingPoints = S.LoadingNumberInfo
                                   .Where(lp => lp.SupplierToProfileId == P.SupplierToProfileId && lp.ClientName == l.ClientName)
                                   .GroupBy(lp => new { lp.LoadingPoint, lp.LoadingPointNumber }) 
                                   .Select(g => g.First())
                                   .Select(lp => new SupplierLoadingNumberLoadingPoint
                                   {
                                       LoadingPoint = lp.LoadingPoint,
                                       LoadingPointNumber = lp.LoadingPointNumber,
                                       LoadingNumberInfo = S.LoadingNumberInfo
                                       .Where(ln => ln.SupplierToProfileId == P.SupplierToProfileId && lp.SupplierToLoadingPointId == ln.SupplierToLoadingPointId
                                        && ln.ClientName == l.ClientName)
                                       .Select(sln => new SupplierLoadingNumberInfo
                                       {
                                           Product = sln.Product,
                                           Article = sln.Article,
                                           LoadingNumber = sln.LoadingNumber,
                                           PIN = sln.Pin
                                       }).ToList()
                                   }).ToList()

                               }).ToList()
                            };
                            
                        }).ToList(),                                            
                    };
                }).ToList();

                var supplierResponse = new SupplierResponse();
                supplierResponse.suppliers = response;
                supplierResponse.totalRecords = totalRecords;

                return supplierResponse;
            }

            catch (Exception ex)
            {
                throw;
            }
        }
    }
    public class SupplierResponse
    {
        public List<SupplierData> suppliers { get; set; }
        public int totalRecords { get; set; }
    }
}
