import sys
import pytest
from mspasspy.db.client import DBClient
from mspasspy.db.database import Database
from mspasspy.util.Undertaker import Undertaker
from mspasspy.ccore.seismic import (
    TimeSeries,
    Seismogram,
    TimeSeriesEnsemble,
    SeismogramEnsemble,
)
from mspasspy.ccore.utility import (
    ErrorSeverity,
    MsPASSError,
    Metadata,
)

sys.path.append("python/tests")
sys.path.append("python/mspasspy/util/")

from helper import (
    get_live_seismogram,
    get_live_timeseries,
    get_live_timeseries_ensemble,
    get_live_seismogram_ensemble,
)


class TestUnderTaker:
    def setup_class(self):
        client = DBClient("localhost")
        self.db = Database(client, "test_undertaker")
        self.aborted_data_collection = "abortions"
        self.regular_data_collection = "cemetery"
        self.undertaker = Undertaker(
            self.db,
            regular_data_collection="cemetery",
            aborted_data_collection="abortions",
            data_tag="undertaker_dead",
        )

    def test_constructor(self):
        # Test with valid database instance
        self.undertaker = Undertaker(
            self.db,
            regular_data_collection="cemetery",
            aborted_data_collection="abortions",
            data_tag="undertaker_dead",
        )
        assert isinstance(self.undertaker, Undertaker)

        # Test with invalid database instance
        with pytest.raises(TypeError):
            undertaker_invalid = Undertaker("invalid_db")

        # Test with no data tag
        undertaker_no_data_tag = Undertaker(self.db)
        assert undertaker_no_data_tag.data_tag is None

        # Test with invalid data tag, in this case data_tag will not be set in undertaker
        with pytest.raises(AttributeError):
            undertaker_invalid_data_tag = Undertaker(self.db, data_tag=123)
            print(undertaker_invalid_data_tag.data_tag)

    def test_is_abortion(self):
        # Test with a TimeSeries object marked as abortion
        ts_abortion = get_live_timeseries()
        ts_abortion["is_abortion"] = True
        assert self.undertaker._is_abortion(ts_abortion) == True

        # Test with a TimeSeries object not marked as abortion
        ts_no_abortion = get_live_timeseries()
        ts_no_abortion["is_abortion"] = False
        assert self.undertaker._is_abortion(ts_no_abortion) == False

        # Test with a TimeSeries object without the is_abortion attribute
        ts_no_attribute = get_live_timeseries()
        assert self.undertaker._is_abortion(ts_no_attribute) == False

        # Test with a Seismogram object marked as abortion
        sg_abortion = get_live_seismogram()
        sg_abortion["is_abortion"] = True
        assert self.undertaker._is_abortion(sg_abortion) == True

        # Test with a Seismogram object not marked as abortion
        sg_no_abortion = get_live_seismogram()
        sg_no_abortion["is_abortion"] = False
        assert self.undertaker._is_abortion(sg_no_abortion) == False

        # Test with a Seismogram object without the is_abortion attribute
        sg_no_attribute = get_live_seismogram()
        assert self.undertaker._is_abortion(sg_no_attribute) == False

        # Test with an invalid type - expecting a TypeError
        with pytest.raises(TypeError):
            self.undertaker._is_abortion("not_a_valid_type")

    def test_handle_abortion(self):
        # Test with TimeSeries object
        ts = get_live_timeseries()
        ts.elog.log_error("sample error", str("message"), ErrorSeverity.Invalid)
        num_docs = self.db[self.aborted_data_collection].count_documents({})
        result = self.undertaker.handle_abortion(ts)
        # Check the database for correct entry
        assert (num_docs + 1) == self.db[self.aborted_data_collection].count_documents(
            {}
        )

        # Test with Seismogram object
        sg = get_live_seismogram()
        sg.elog.log_error("sample error", str("message"), ErrorSeverity.Invalid)
        result = self.undertaker.handle_abortion(sg)
        # Check the database for correct entry
        assert (num_docs + 2) == self.db[self.aborted_data_collection].count_documents(
            {}
        )

        # Test with a Metadata object
        md = Metadata()
        md["some_key"] = "some_value"
        result = self.undertaker.handle_abortion(md, type="MetadataType")
        # Check the database for correct entry
        assert (num_docs + 3) == self.db[self.aborted_data_collection].count_documents(
            {}
        )

        # Test with a dictionary
        doc = {"key1": "value1", "key2": "value2"}
        result = self.undertaker.handle_abortion(doc, type="DictType")
        # Check the database for correct entry
        assert (num_docs + 4) == self.db[self.aborted_data_collection].count_documents(
            {}
        )

        # Test with a dictionary without type parameter
        result = self.undertaker.handle_abortion(doc)
        # Check that type is set to 'unknown'
        last_document = list(
            self.db[self.aborted_data_collection].find().sort("_id", -1).limit(1)
        )[0]
        assert last_document["type"] == "unknown"

        # Test with an invalid type - expecting a TypeError
        with pytest.raises(TypeError):
            self.undertaker.handle_abortion("not_a_valid_type")

    def test_mummify(self):
        # Test mummify with a dead TimeSeries object
        dead_ts = get_live_timeseries()
        dead_ts.kill()
        result = self.undertaker.mummify(dead_ts, post_elog=True, post_history=False)
        assert result.elog.size() == 0
        assert result.npts == 0
        assert "error_log" in result

        # Test mummify with a live TimeSeries object
        live_ts = get_live_timeseries()
        live_ts.live = True
        result = self.undertaker.mummify(live_ts)
        assert result.live and result.npts != 0

        # Test mummify with a dead Seismogram object
        dead_sg = get_live_seismogram()
        dead_sg.kill()
        result = self.undertaker.mummify(dead_sg, post_elog=False, post_history=True)
        assert result.npts == 0
        assert result.is_empty()
        assert "processing_history" in result

        # Test mummify with an ensemble containing both live and dead members
        ensemble = TimeSeriesEnsemble()
        live_member = get_live_timeseries()
        dead_member = get_live_timeseries()
        dead_member.kill()
        ensemble.member.append(live_member)
        ensemble.member.append(dead_member)
        result = self.undertaker.mummify(ensemble)
        assert len(result.member) == 2
        assert result.member[1].npts == 0  # Dead member should be mummified

        # Test mummify with an entirely dead ensemble
        dead_ensemble = TimeSeriesEnsemble()
        for _ in range(3):
            member = get_live_timeseries()
            dead_ensemble.member.append(member)
        dead_ensemble.kill()  # Mark the entire ensemble as dead
        result = self.undertaker.mummify(dead_ensemble)
        for member in result.member:
            assert member.dead()
            assert member.npts == 0  # All members should be mummified

        # Test with an invalid type - expecting a TypeError
        with pytest.raises(TypeError):
            self.undertaker.mummify("not_a_valid_type")

    def test_bring_out_your_dead(self):
        # Create a mixed ensemble with both live and dead members
        ensemble = TimeSeriesEnsemble()
        live_member = get_live_timeseries()
        dead_member = get_live_timeseries()
        dead_member.kill()
        ensemble.member.append(live_member)
        ensemble.member.append(dead_member)
        ensemble.live = True

        live_ensemble, dead_ensemble = self.undertaker.bring_out_your_dead(
            ensemble, bury=True
        )
        assert len(live_ensemble.member) == 1
        assert len(dead_ensemble.member) == 1
        assert live_ensemble.member[0].live
        assert dead_ensemble.member[0].dead()

        # Test with a dead ensemble
        dead_ensemble = SeismogramEnsemble()
        for _ in range(3):
            member = Seismogram()
            member.kill()
            dead_ensemble.member.append(member)
        dead_ensemble.kill()  # Mark the entire ensemble as dead
        live_ensemble, dead_ensemble = self.undertaker.bring_out_your_dead(
            dead_ensemble
        )
        assert len(live_ensemble.member) == 0
        assert len(dead_ensemble.member) == 3

        # Test with an invalid type - expecting a TypeError
        with pytest.raises(TypeError):
            self.undertaker.bring_out_your_dead("not_a_valid_type")

    def test_bury(self):
        # Test burying a dead TimeSeries object
        dead_ts = TimeSeries()
        dead_ts.kill()
        assert not self.undertaker._is_abortion(dead_ts)
        result = self.undertaker.bury(
            dead_ts, save_history=True, mummify_atomic_data=True
        )
        assert dead_ts.dead()
        # assert that the elog was saved to the database
        elog_doc = self.db[self.regular_data_collection].find_one(
            {"data_tag": "undertaker_dead"}
        )
        assert elog_doc is not None
        # assert that the data was mummified
        assert dead_ts.npts == 0
        # assert that the history was saved
        assert (
            self.db["history_object"].find_one({"alg_name": "Undertaker.bury"})
            is not None
        )

        # Test burying a dead Seismogram object
        dead_sg = Seismogram()
        dead_sg["is_abortion"] = True
        dead_sg.kill()
        result = self.undertaker.bury_the_dead(dead_sg)
        assert dead_sg.dead()

        # Test burying a TimeSeriesEnsemble with both live and dead members
        tse = TimeSeriesEnsemble()
        live_member = get_live_timeseries()
        dead_member = get_live_timeseries()
        dead_member.kill()
        tse.member.append(live_member)
        tse.member.append(dead_member)
        tse.live = True
        result = self.undertaker.bury(tse)
        assert len(result.member) == 1  # Assuming dead members are removed
        assert result.live

        # Test burying a SeismogramEnsemble with both live and dead members
        sge = SeismogramEnsemble()
        live_member = get_live_seismogram()
        dead_member = get_live_seismogram()
        dead_member.kill()
        sge.member.append(live_member)
        sge.member.append(dead_member)
        sge.live = True
        result = self.undertaker.bury(sge)
        assert len(result.member) == 1
        assert result.live

        # Test burying a dead SeismogramEnsemble
        sge_dead = SeismogramEnsemble()
        sge_dead.member.append(get_live_seismogram())
        sge_dead.member.append(get_live_seismogram())
        sge_dead.kill()
        result = self.undertaker.bury(sge_dead)
        assert result.dead()
        assert len(result.member) == 0

        # Test with an invalid type - expecting a TypeError
        with pytest.raises(TypeError):
            self.undertaker.bury("not_a_valid_type")

    def test_cremate(self):
        # Test cremate with a live TimeSeries object
        live_ts = get_live_timeseries()
        live_ts.live = True
        result = self.undertaker.cremate(live_ts)
        assert result is live_ts

        # Test cremate with a dead TimeSeries object
        dead_ts = get_live_timeseries()
        dead_ts.kill()
        dead_ts["is_abortion"] = True
        result = self.undertaker.cremate(dead_ts)
        assert isinstance(result, TimeSeries)
        assert result.toTrace() == TimeSeries().toTrace()

        # Test cremate with a live Seismogram object
        live_sg = get_live_seismogram()
        live_sg.live = True
        result = self.undertaker.cremate(live_sg)
        assert result is live_sg

        # Test cremate with a dead Seismogram object
        dead_sg = get_live_seismogram()
        dead_sg.kill()
        result = self.undertaker.cremate(dead_sg)
        assert isinstance(result, Seismogram)
        assert result.toStream() == Seismogram().toStream()

        # Test cremate with a TimeSeriesEnsemble containing both live and dead members
        tse = TimeSeriesEnsemble()
        live_member = get_live_timeseries()
        dead_member = get_live_timeseries()
        dead_member.kill()
        dead_member["is_abortion"] = True
        tse.member.append(live_member)
        tse.member.append(dead_member)
        tse.live = True
        result = self.undertaker.cremate(tse)
        assert len(result.member) == 1  # Assuming dead members are removed
        assert result.live

        # Test cremate with a SeismogramEnsemble containing both live and dead members
        sge = SeismogramEnsemble()
        live_member = get_live_seismogram()
        dead_member = get_live_seismogram()
        dead_member.kill()
        sge.member.append(live_member)
        sge.member.append(dead_member)
        sge.live = True
        result = self.undertaker.cremate(sge)
        assert len(result.member) == 1
        assert result.live

        # Test burying a dead SeismogramEnsemble
        sge_dead = SeismogramEnsemble()
        sge_dead.member.append(get_live_seismogram())
        sge_dead.member.append(get_live_seismogram())
        sge_dead.kill()
        result = self.undertaker.cremate(sge_dead)
        assert result.dead()
        assert len(result.member) == 0

        # Test with an invalid type - expecting a TypeError
        with pytest.raises(TypeError):
            self.undertaker.cremate("not_a_valid_type")
