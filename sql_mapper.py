from sqlalchemy import create_engine, Column, DateTime, String
from sqlalchemy.ext.declarative import declarative_base

engine = create_engine('sqlite:///contract_opportunities.db', echo = False)
Base = declarative_base()

class Contract_Opportunity(Base):
    __tablename__ = 'ContractOpportunities'

    # the values of some numeric and datetime columns do not have a consistant format, e.g. the Award column can have values with and without the $ prefix
    # so all the columns are treated as string for now.
    NoticeId = Column(String, primary_key=True)
    Title = Column(String)
    SolNo  = Column(String)
    DeptOrIndAgency = Column(String)
    CGAC = Column(String)
    SubTier = Column(String)
    FpdsCode = Column(String)
    Office = Column(String)
    AacCode = Column(String)
    PostedDate  = Column(String)
    Type = Column(String)
    BaseType = Column(String)
    ArchiveType = Column(String)
    ArchiveDate = Column(String)
    SetASideCode = Column(String)
    SetASide = Column(String)
    ResponseDeadLine = Column(String)
    NaicsCode = Column(String)
    ClassificationCode = Column(String)
    PopStreetAddress = Column(String)
    PopCity = Column(String)
    PopState = Column(String)
    PopZip = Column(String)
    PopCountry = Column(String)
    Active = Column(String)
    AwardNumber = Column(String)
    AwardDate = Column(String)
    Award = Column(String)
    Awardee = Column(String)
    PrimaryContactTitle = Column(String)
    PrimaryContactFullname = Column(String)
    PrimaryContactEmail = Column(String)
    PrimaryContactPhone = Column(String)
    PrimaryContactFax = Column(String)
    SecondaryContactTitle = Column(String)
    SecondaryContactFullname = Column(String)
    SecondaryContactEmail = Column(String)
    SecondaryContactPhone = Column(String)
    SecondaryContactFax = Column(String)
    OrganizationType = Column(String)
    State = Column(String)
    City = Column(String)
    ZipCode = Column(String)
    CountryCode = Column(String)
    AdditionalInfoLink = Column(String)
    Link = Column(String)
    Description = Column(String)

Base.metadata.create_all(engine)

class sql_mapper():
    from sqlalchemy.orm import sessionmaker

    Session = sessionmaker(bind = engine)
    session = Session()

    def __convert_df_row_to_contract_opportunity_object(self, row) -> Contract_Opportunity:
        co = Contract_Opportunity(
            NoticeId = row['NoticeId'],
            Title = row['Title'],
            SolNo  = row['Sol#'],
            DeptOrIndAgency = row['Department/Ind.Agency'],
            CGAC = row['CGAC'],
            SubTier = row['Sub-Tier'],
            FpdsCode = row['FPDS Code'],
            Office = row['Office'],
            AacCode = row['AAC Code'],
            PostedDate  = row['PostedDate'],
            Type = row['Type'],
            BaseType = row['BaseType'],
            ArchiveType = row['ArchiveType'],
            ArchiveDate = row['ArchiveDate'],
            SetASideCode = row['SetASideCode'],
            SetASide = row['SetASide'],
            ResponseDeadLine = row['ResponseDeadLine'],
            NaicsCode = row['NaicsCode'],
            ClassificationCode = row['ClassificationCode'],
            PopStreetAddress = row['PopStreetAddress'],
            PopCity = row['PopCity'],
            PopState = row['PopState'],
            PopZip = row['PopZip'],
            PopCountry = row['PopCountry'],
            Active = row['Active'],
            AwardNumber = row['AwardNumber'],
            AwardDate = row['AwardDate'],
            Award = row['Award$'],
            Awardee = row['Awardee'],
            PrimaryContactTitle = row['PrimaryContactTitle'],
            PrimaryContactFullname = row['PrimaryContactFullname'],
            PrimaryContactEmail = row['PrimaryContactEmail'],
            PrimaryContactPhone = row['PrimaryContactPhone'],
            PrimaryContactFax = row['PrimaryContactFax'],
            SecondaryContactTitle = row['SecondaryContactTitle'],
            SecondaryContactFullname = row['SecondaryContactFullname'],
            SecondaryContactEmail = row['SecondaryContactEmail'],
            SecondaryContactPhone = row['SecondaryContactPhone'],
            SecondaryContactFax = row['SecondaryContactFax'],
            OrganizationType = row['OrganizationType'],
            State = row['State'],
            City = row['City'],
            ZipCode = row['ZipCode'],
            CountryCode = row['CountryCode'],
            AdditionalInfoLink = row['AdditionalInfoLink'],
            Link = row['Link'],
            Description = row['Description']
        )
        return co

    def add_contract_opportunity_records_to_DB(self, years: []):
        from utilities import load_contract_opportunities_archived_data

        df = load_contract_opportunities_archived_data(years)
        if (len(df) > 0):
            data_to_save = [self.__convert_df_row_to_contract_opportunity_object(row) for index, row in df.iterrows()]
            self.session.add_all(data_to_save)
            self.session.commit()
