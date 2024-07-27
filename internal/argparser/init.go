package argparser

import (
	"flag"
	"fmt"
	"os"
)

const VERSION = "1.0.1"

var TypeInt = "int"
var TypeBool = "bool"
var TypeUint = "uint"
var TypeString = "string"
var TypeUint64 = "uint64"
var TypeFloat64 = "float64"

type argInfo struct {
	typo string
	val  any
}

type ArgParser struct {
	args    [][]any
	flatSet *flag.FlagSet
}

func NewArgParser(args [][]any) *ArgParser {
	return &ArgParser{args: args}
}

func parseTypeArgValue(typeArg *argInfo) any {
	switch typeArg.typo {
	case TypeInt:
		return *(typeArg.val.(*int))
	case TypeBool:
		return *(typeArg.val.(*bool))
	case TypeUint:
		return *(typeArg.val.(*uint))
	case TypeString:
		return *(typeArg.val.(*string))
	case TypeUint64:
		return *(typeArg.val.(*uint64))
	case TypeFloat64:
		return *(typeArg.val.(*float64))
	default:
		return 0
	}
}

func (ap *ArgParser) Parse(tag string) (map[string]any, error) {
	ap.flatSet = flag.NewFlagSet(tag, flag.ExitOnError)

	args := map[string]*argInfo{}
	for _, arg := range ap.args {
		name := arg[0].(string)
		typo := arg[1].(string)
		help := arg[2].(string)
		def := arg[3]

		switch typo {
		case TypeInt:
			defVal, ok := def.(int)
			if !ok {
				return nil, defValError(name, def)
			}

			val := ap.flatSet.Int(name, defVal, help)
			args[name] = &argInfo{typo: typo, val: val}
		case TypeBool:
			defVal, ok := def.(bool)
			if !ok {
				return nil, defValError(name, def)
			}

			val := ap.flatSet.Bool(name, defVal, help)
			args[name] = &argInfo{typo: typo, val: val}
		case TypeUint:
			defVal, ok := def.(uint)
			if !ok {
				return nil, defValError(name, def)
			}

			val := ap.flatSet.Uint(name, defVal, help)
			args[name] = &argInfo{typo: typo, val: val}
		case TypeString:
			defVal, ok := def.(string)
			if !ok {
				return nil, defValError(name, def)
			}

			val := ap.flatSet.String(name, defVal, help)
			args[name] = &argInfo{typo: typo, val: val}
		case TypeUint64:
			defVal, ok := def.(uint64)
			if !ok {
				return nil, defValError(name, def)
			}

			val := ap.flatSet.Uint64(name, defVal, help)
			args[name] = &argInfo{typo: typo, val: val}
		case TypeFloat64:
			defVal, ok := def.(float64)
			if !ok {
				return nil, defValError(name, def)
			}

			val := ap.flatSet.Float64(name, defVal, help)
			args[name] = &argInfo{typo: typo, val: val}
		default:
			return nil, fmt.Errorf("不支持此参数类型，%s, type: %v", name, typo)
		}
	}

	err := ap.flatSet.Parse(os.Args[1:])
	if err != nil {
		return nil, fmt.Errorf("解析失败：%v", err)
	}

	var ret = map[string]any{}
	for key, typeArg := range args {
		value := parseTypeArgValue(typeArg)
		ret[key] = value
	}

	return ret, nil
}

func (ap *ArgParser) PrintHelp() {
	ap.flatSet.Usage()
}

func defValError(name string, def any) error {
	return fmt.Errorf("参数默认值类型错误, %s, def: %v", name, def)
}
