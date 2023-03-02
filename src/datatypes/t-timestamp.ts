import { DataType, nil, QueryError, _IType } from "../interfaces-private";
import { TypeBase } from "./datatype-base";
import { Evaluator } from "../evaluator";
import moment from "moment";
import { parseTime, nullIsh } from "../utils";

export class TimestampType extends TypeBase<Date> {
  constructor(
    readonly primary: DataType,
    readonly precision: number | null = null
  ) {
    super();
  }

  get name(): string {
    if (!nullIsh(this.precision)) {
      return `${this.primary}(${this.precision})`;
    }
    switch (this.primary) {
      case DataType.timestamp:
        return "timestamp without time zone";
      case DataType.timestamptz:
        return "timestamp with time zone";
      case DataType.date:
        return "date";
      case DataType.time:
        return "time without time zone";
      case DataType.timetz:
        return "time with time zone";
    }
    return this.primary;
  }

  doCanCast(to: _IType) {
    switch (to.primary) {
      case DataType.timestamp:
      case DataType.timestamptz:
      case DataType.date:
        return (
          this.primary !== DataType.time && this.primary !== DataType.timetz
        );
      case DataType.time:
        return this.primary !== DataType.date;
      case DataType.timetz:
        return (
          this.primary !== DataType.date && this.primary !== DataType.timestamp
        );
    }
    return null;
  }

  doCanConvertImplicit(to: _IType) {
    switch (to.primary) {
      case DataType.timestamp:
        return (
          this.primary === DataType.timestamp || this.primary === DataType.date
        );
      case DataType.timestamptz:
        return this.primary !== DataType.time;
      case DataType.date:
        return this.primary === DataType.date;
      case DataType.time:
        return this.primary === DataType.time; // nothing can implicitly cast to time
    }
    return false;
  }

  doCast(value: Evaluator, to: _IType) {
    switch (to.primary) {
      case DataType.timestamp:
      case DataType.timestamptz:
        return value;
      case DataType.date:
        return value.setConversion(
          (raw) => moment.utc(raw).startOf("day").toDate(),
          (toDate) => ({ toDate })
        );
      case DataType.time:
      case DataType.timetz:
        return value.setConversion(
          (raw) => moment.utc(raw).format("HH:mm:ss") + ".000000",
          (toDate) => ({ toDate })
        );
    }
    throw new Error("Unexpected cast error");
  }

  doCanBuildFrom(from: _IType) {
    switch (from.primary) {
      case DataType.text:
        return true;
    }
    return false;
  }

  doBuildFrom(value: Evaluator, from: _IType): Evaluator<Date> | nil {
    switch (from.primary) {
      case DataType.text:
        switch (this.primary) {
          case DataType.timestamp:
          case DataType.timestamptz:
            return value.setConversion(
              (str) => {
                if (`${str}`.toLowerCase() === "now") {
                  return moment().toDate();
                }
                const conv = moment.utc(str);
                if (!conv.isValid()) {
                  throw new QueryError(`Invalid timestamp format: ` + str);
                }
                return conv.toDate();
              },
              (toTs) => ({ toTs, t: this.primary })
            );
          case DataType.date:
            return value.setConversion(
              (str) => {
                const conv = moment.utc(str);
                if (!conv.isValid()) {
                  throw new QueryError(`Invalid timestamp format: ` + str);
                }
                return conv.startOf("day").toDate();
              },
              (toDate) => ({ toDate })
            );
          case DataType.time:
          case DataType.timetz:
            return value.setConversion(
              (str) => {
                parseTime(str); // will throw an error if invalid format
                return str;
              },
              (toTime) => ({ toTime, t: this.primary })
            );
        }
    }
    return null;
  }

  doEquals(a: any, b: any): boolean {
    return Math.abs(moment(a).diff(moment(b))) < 0.1;
  }
  doGt(a: any, b: any): boolean {
    return moment(a).diff(moment(b)) > 0;
  }
  doLt(a: any, b: any): boolean {
    return moment(a).diff(moment(b)) < 0;
  }
}
