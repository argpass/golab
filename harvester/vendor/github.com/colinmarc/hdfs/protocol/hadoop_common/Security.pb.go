// Code generated by protoc-gen-go.
// source: Security.proto
// DO NOT EDIT!

package hadoop_common

import proto "github.com/golang/protobuf/proto"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = math.Inf

// *
// Security token identifier
type TokenProto struct {
	Identifier       []byte  `protobuf:"bytes,1,req,name=identifier" json:"identifier,omitempty"`
	Password         []byte  `protobuf:"bytes,2,req,name=password" json:"password,omitempty"`
	Kind             *string `protobuf:"bytes,3,req,name=kind" json:"kind,omitempty"`
	Service          *string `protobuf:"bytes,4,req,name=service" json:"service,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *TokenProto) Reset()         { *m = TokenProto{} }
func (m *TokenProto) String() string { return proto.CompactTextString(m) }
func (*TokenProto) ProtoMessage()    {}

func (m *TokenProto) GetIdentifier() []byte {
	if m != nil {
		return m.Identifier
	}
	return nil
}

func (m *TokenProto) GetPassword() []byte {
	if m != nil {
		return m.Password
	}
	return nil
}

func (m *TokenProto) GetKind() string {
	if m != nil && m.Kind != nil {
		return *m.Kind
	}
	return ""
}

func (m *TokenProto) GetService() string {
	if m != nil && m.Service != nil {
		return *m.Service
	}
	return ""
}

type GetDelegationTokenRequestProto struct {
	Renewer          *string `protobuf:"bytes,1,req,name=renewer" json:"renewer,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *GetDelegationTokenRequestProto) Reset()         { *m = GetDelegationTokenRequestProto{} }
func (m *GetDelegationTokenRequestProto) String() string { return proto.CompactTextString(m) }
func (*GetDelegationTokenRequestProto) ProtoMessage()    {}

func (m *GetDelegationTokenRequestProto) GetRenewer() string {
	if m != nil && m.Renewer != nil {
		return *m.Renewer
	}
	return ""
}

type GetDelegationTokenResponseProto struct {
	Token            *TokenProto `protobuf:"bytes,1,opt,name=token" json:"token,omitempty"`
	XXX_unrecognized []byte      `json:"-"`
}

func (m *GetDelegationTokenResponseProto) Reset()         { *m = GetDelegationTokenResponseProto{} }
func (m *GetDelegationTokenResponseProto) String() string { return proto.CompactTextString(m) }
func (*GetDelegationTokenResponseProto) ProtoMessage()    {}

func (m *GetDelegationTokenResponseProto) GetToken() *TokenProto {
	if m != nil {
		return m.Token
	}
	return nil
}

type RenewDelegationTokenRequestProto struct {
	Token            *TokenProto `protobuf:"bytes,1,req,name=token" json:"token,omitempty"`
	XXX_unrecognized []byte      `json:"-"`
}

func (m *RenewDelegationTokenRequestProto) Reset()         { *m = RenewDelegationTokenRequestProto{} }
func (m *RenewDelegationTokenRequestProto) String() string { return proto.CompactTextString(m) }
func (*RenewDelegationTokenRequestProto) ProtoMessage()    {}

func (m *RenewDelegationTokenRequestProto) GetToken() *TokenProto {
	if m != nil {
		return m.Token
	}
	return nil
}

type RenewDelegationTokenResponseProto struct {
	NewExpiryTime    *uint64 `protobuf:"varint,1,req,name=newExpiryTime" json:"newExpiryTime,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *RenewDelegationTokenResponseProto) Reset()         { *m = RenewDelegationTokenResponseProto{} }
func (m *RenewDelegationTokenResponseProto) String() string { return proto.CompactTextString(m) }
func (*RenewDelegationTokenResponseProto) ProtoMessage()    {}

func (m *RenewDelegationTokenResponseProto) GetNewExpiryTime() uint64 {
	if m != nil && m.NewExpiryTime != nil {
		return *m.NewExpiryTime
	}
	return 0
}

type CancelDelegationTokenRequestProto struct {
	Token            *TokenProto `protobuf:"bytes,1,req,name=token" json:"token,omitempty"`
	XXX_unrecognized []byte      `json:"-"`
}

func (m *CancelDelegationTokenRequestProto) Reset()         { *m = CancelDelegationTokenRequestProto{} }
func (m *CancelDelegationTokenRequestProto) String() string { return proto.CompactTextString(m) }
func (*CancelDelegationTokenRequestProto) ProtoMessage()    {}

func (m *CancelDelegationTokenRequestProto) GetToken() *TokenProto {
	if m != nil {
		return m.Token
	}
	return nil
}

type CancelDelegationTokenResponseProto struct {
	XXX_unrecognized []byte `json:"-"`
}

func (m *CancelDelegationTokenResponseProto) Reset()         { *m = CancelDelegationTokenResponseProto{} }
func (m *CancelDelegationTokenResponseProto) String() string { return proto.CompactTextString(m) }
func (*CancelDelegationTokenResponseProto) ProtoMessage()    {}

func init() {
}
